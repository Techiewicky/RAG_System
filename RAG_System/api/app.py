import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor
from psycopg2.extensions import register_adapter, AsIs

from openai import OpenAI
from langdetect import detect, LangDetectException
import re

# Vector adapter registration
def adapt_vector(lst):
    return AsIs(f"'{lst}'::vector")
register_adapter(list, adapt_vector)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('app.log')]
)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://adm:adm@db:5432/database")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    logging.error("OPENAI_API_KEY is not set!")
    raise ValueError("Missing OpenAI API key")

client = OpenAI(api_key=OPENAI_API_KEY)
app = FastAPI()

# Models
class QueryRequest(BaseModel):
    query: str
    k: int = 5
    score_threshold: float = 0.75

class QueryResponse(BaseModel):
    answer: str
    sources: List[dict]
    confidence: float

# Database connection pool
CONN_POOL = None
def get_db_connection():
    global CONN_POOL
    if not CONN_POOL:
        CONN_POOL = psycopg2.pool.SimpleConnectionPool(1, 20, dsn=DATABASE_URL)
    return CONN_POOL.getconn()

def release_db_connection(conn):
    CONN_POOL.putconn(conn)

# Enhanced language detection
def detect_language(text: str) -> str:
    text = re.sub(r'\s+', ' ', text.strip())
    if not text:
        return 'en'
    
    try:
        lang = detect(text)
        return 'ar' if lang == 'ar' else 'en'
    except (LangDetectException, UnicodeDecodeError):
        pass
    
    arabic_chars = sum(1 for c in text if '\u0600' <= c <= '\u06FF')
    return 'ar' if arabic_chars / len(text) >= 0.15 else 'en'

# Optimized embedding cache
EMBED_CACHE = {}
def get_embedding(text: str) -> List[float]:
    cache_key = text[:512]
    if cache_key in EMBED_CACHE:
        return EMBED_CACHE[cache_key]
    
    text = text.strip()[:8192]
    if not text:
        return [0.0] * 1536
    
    try:
        resp = client.embeddings.create(input=[text], model="text-embedding-ada-002")
        embedding = resp.data[0].embedding
        EMBED_CACHE[cache_key] = embedding
        return embedding
    except Exception as e:
        logging.error(f"Embedding error: {e}")
        return [0.0] * 1536

# Optimized location search using UNION ALL
def find_top_location(query_emb: List[float], k: int, threshold: float):
    conn = None  # Initialize conn
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                WITH combined AS (
                    SELECT 
                        'region' AS type, 
                        region_id AS id, 
                        name_ar, 
                        name_en,
                        1 - (region_embedding <=> %s) AS score
                    FROM regions
                    WHERE 1 - (region_embedding <=> %s) >= %s
                    
                    UNION ALL
                    
                    SELECT 
                        'governorate' AS type,
                        gov_id AS id,
                        name_ar,
                        name_en,
                        1 - (gov_embedding <=> %s) AS score
                    FROM governorates
                    WHERE 1 - (gov_embedding <=> %s) >= %s
                )
                SELECT * FROM combined
                ORDER BY score DESC
                LIMIT %s
            """, (query_emb, query_emb, threshold, query_emb, query_emb, threshold, k))
            
            results = [{
                'type': row['type'],
                'id': row['id'],
                'name_ar': row['name_ar'],
                'name_en': row['name_en'],
                'score': float(row['score'])
            } for row in cur.fetchall()]
            
            return results
    except Exception as e:
        logging.error(f"Location search error: {e}")
        return []
    finally:
        if conn:
            release_db_connection(conn)

# Optimized data fetching with single query
def fetch_location_data(location_type: str, location_id: str):
    conn = None  # Initialize conn
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            if location_type == 'region':
                cur.execute("""
                    SELECT
                        a.alert_id,
                        a.alert_title,
                        a.alert_type_ar,
                        a.alert_type_en,
                        a.status_ar,
                        a.status_en,
                        ARRAY_AGG(DISTINCT g.name_ar) AS gov_names_ar,
                        ARRAY_AGG(DISTINCT g.name_en) AS gov_names_en,
                        ARRAY_AGG(DISTINCT h.description_ar) AS hazards_ar,
                        ARRAY_AGG(DISTINCT h.description_en) AS hazards_en
                    FROM alerts a
                    JOIN alert_governorates ag ON a.alert_id = ag.alert_id
                    JOIN governorates g ON ag.gov_id = g.gov_id
                    LEFT JOIN alert_hazards ah ON a.alert_id = ah.alert_id
                    LEFT JOIN hazards h ON ah.hazard_id = h.hazard_id
                    WHERE g.region_id = %s
                    GROUP BY a.alert_id
                    ORDER BY a.from_date DESC
                """, (location_id,))
            else:
                cur.execute("""
                    SELECT
                        a.alert_id,
                        a.alert_title,
                        a.alert_type_ar,
                        a.alert_type_en,
                        a.status_ar,
                        a.status_en,
                        ARRAY_AGG(DISTINCT g.name_ar) AS gov_names_ar,
                        ARRAY_AGG(DISTINCT g.name_en) AS gov_names_en,
                        ARRAY_AGG(DISTINCT h.description_ar) AS hazards_ar,
                        ARRAY_AGG(DISTINCT h.description_en) AS hazards_en
                    FROM alerts a
                    JOIN alert_governorates ag ON a.alert_id = ag.alert_id
                    JOIN governorates g ON ag.gov_id = g.gov_id
                    LEFT JOIN alert_hazards ah ON a.alert_id = ah.alert_id
                    LEFT JOIN hazards h ON ah.hazard_id = h.hazard_id
                    WHERE g.gov_id = %s
                    GROUP BY a.alert_id
                    ORDER BY a.from_date DESC
                """, (location_id,))
            
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Data fetch error: {e}")
        return []
    finally:
        if conn:
            release_db_connection(conn)

# Enhanced prompt engineering
def generate_answer(query: str, data: list, language: str) -> str:
    system_prompt = {
        'ar': (
            "أنت مساعد احترافي ثنائي اللغة للتنبيهات الأمنية. الردود يجب أن تكون بالعربية فقط. "
            "المطلوب:\n"
            "1. اذكر نوع التنبيه أولاً\n"
            "2. حالة التنبيه الحالية\n"
            "3. المناطق المتأثرة\n"
            "4. المخاطر المحددة\n"
            "5. إذا لم توجد بيانات، قل ذلك بوضوح\n"
            "6. استخدم تنسيق النقاط بدون ماركداون\n"
            "7. لا تقدم أي معلومات غير موجودة في البيانات\n"
            "مثال:\n"
            "- نوع التنبيه: فيضانات\n"
            "- الحالة: نشط\n"
            "- المناطق: تبوك، ضبا\n"
            "- المخاطر: فيضان سريع، انزلاقات تربة"
        ),
        'en': (
            "You are a professional bilingual safety alert assistant. Responses must be in English. "
            "Requirements:\n"
            "1. Start with alert type\n"
            "2. Current status\n"
            "3. Affected areas\n"
            "4. Specific hazards\n"
            "5. If no data, state this clearly\n"
            "6. Use bullet points without markdown\n"
            "7. Do not provide any information beyond the data\n"
            "Example:\n"
            "- Alert type: Floods\n"
            "- Status: Active\n"
            "- Areas: Tabuk, Duba\n"
            "- Hazards: Flash flooding, land slides"
        )
    }

    user_context = []
    for alert in data:
        alert_type = alert['alert_type_ar' if language == 'ar' else 'alert_type_en']
        status = alert['status_ar' if language == 'ar' else 'status_en']
        areas = alert['gov_names_ar' if language == 'ar' else 'gov_names_en']
        hazards = alert['hazards_ar' if language == 'ar' else 'hazards_en']

        entry = {
            'type': alert_type,
            'status': status,
            'areas': list(set(areas)),
            'hazards': list(set(hazards))
        }
        user_context.append(entry)

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt[language]},
                {"role": "user", "content": f"Query: {query}\nData: {str(user_context)}"}
            ],
            temperature=0.1,
            max_tokens=500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"GPT error: {e}")
        return "حدث خطأ في توليد الرد" if language == 'ar' else "Error generating response"

@app.post("/query", response_model=QueryResponse)
def handle_query(payload: QueryRequest):
    query = payload.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Empty query")

    lang = detect_language(query)
    query_emb = get_embedding(query)
    locations = find_top_location(query_emb, payload.k, payload.score_threshold)

    if not locations:
        return QueryResponse(
            answer="لا توجد نتائج" if lang == 'ar' else "No results found",
            sources=[],
            confidence=0.0
        )

    best_match = locations[0]
    data = fetch_location_data(best_match["type"], best_match["id"])
    
    if not data:
        return QueryResponse(
            answer="لا توجد تنبيهات حالية" if lang == 'ar' else "No current alerts",
            sources=[{
                "type": best_match["type"],
                "id": best_match["id"],
                "name_ar": best_match.get("name_ar", ""),
                "name_en": best_match.get("name_en", ""),
                "score": round(best_match["score"], 2)
            }],
            confidence=round(best_match["score"], 2)
        )

    answer = generate_answer(query, data, lang)
    return QueryResponse(
        answer=answer,
        sources=[{
            "type": best_match["type"],
            "id": best_match["id"],
            "name_ar": best_match.get("name_ar", ""),
            "name_en": best_match.get("name_en", ""),
            "score": round(best_match["score"], 2)
        }],
        confidence=round(best_match["score"], 2)
    )

@app.get("/health")
def health_check():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        release_db_connection(conn)
        return {"status": "OK"}
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Service unavailable")