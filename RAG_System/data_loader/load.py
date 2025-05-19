import os
import sys
import json
import time
import logging
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from typing import List, Optional
from openai import OpenAI

# --------------------------------------------------------------------------
# Logging Configuration
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('load.log')
    ]
)

# --------------------------------------------------------------------------
# Environment Variables and Settings
# --------------------------------------------------------------------------



# PostgreSQL connection settings.
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_DB = os.getenv("POSTGRES_DB", "database")
POSTGRES_USER = os.getenv("POSTGRES_USER", "adm")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "adm")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
# The JSON_DIR environment variable must contain the URL of the GeoJSON file.
JSON_URL = os.getenv("JSON_URL")
if not JSON_URL:
    raise ValueError("Missing JSON_DIR environment variable (should be the GeoJSON URL)")

# Derive the destination file name from the URL (e.g. "latest.geojson").
DEST_FILE = os.path.basename(JSON_URL)

TIMEOUT = 10  # Timeout (in seconds) for the HTTP request.

# OpenAI API key (required).
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("Missing OPENAI_API_KEY environment variable")

# Initialize OpenAI client.
client = OpenAI(api_key=OPENAI_API_KEY, timeout=30.0)

# --------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------
def download_file(url: str, dest: str, timeout: int = 10, chunk_size: int = 8192) -> None:
    """
    Download a file from the specified URL and save it to the destination.
    
    If the destination file exists, it is removed before writing a new copy.
    The file is downloaded in chunks to avoid high memory usage.
    
    Parameters:
        url (str): The URL to download the file from.
        dest (str): The destination file path.
        timeout (int): Timeout (in seconds) for the HTTP request.
        chunk_size (int): The size (in bytes) of each chunk.
    """
    logging.info("Starting download from: %s", url)
    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()  # Raise an error on bad HTTP status codes.
    except requests.exceptions.RequestException as req_err:
        logging.error("HTTP request failed: %s", req_err)
        sys.exit(1)

    # Remove the file if it already exists.
    if os.path.exists(dest):
        try:
            os.remove(dest)
            logging.info("Removed existing file: %s", dest)
        except Exception as e:
            logging.error("Could not remove existing file '%s': %s", dest, e)
            sys.exit(1)

    # Write the downloaded content to file in chunks.
    try:
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # Filter out keep-alive chunks.
                    f.write(chunk)
        logging.info("File downloaded successfully and saved to: %s", dest)
    except Exception as e:
        logging.error("Error saving file '%s': %s", dest, e)
        sys.exit(1)

def get_embedding(text: str, model: str = "text-embedding-ada-002") -> List[float]:
    """
    Retrieve embeddings from OpenAI with simple retry logic.
    Returns a zero-vector if the text is empty.
    
    Parameters:
        text (str): The text to embed.
        model (str): The OpenAI model to use.
    """
    text = text.strip()
    if not text:
        return [0.0] * 1536

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.embeddings.create(input=[text], model=model)
            return response.data[0].embedding
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Embedding failed after {max_retries} attempts: {e}")
                raise
            wait = 2 ** attempt
            logging.warning(f"Embedding request failed, retrying in {wait}s...")
            time.sleep(wait)

def wait_for_db(max_retries: int = 5):
    """
    Wait for the PostgreSQL database to be ready with exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                port=POSTGRES_PORT
            )
            conn.close()
            logging.info("Database connection successful.")
            return
        except psycopg2.OperationalError as e:
            if attempt == max_retries - 1:
                logging.error("Database connection failed after %d attempts", max_retries)
                raise
            wait = 2 ** attempt
            logging.warning("Database not ready, retrying in %ds...", wait)
            time.sleep(wait)

def safe_float(value: Optional[str]) -> Optional[float]:
    """
    Safely convert a string to a float. Return None if conversion fails.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def parse_date(dt_str: str) -> Optional[datetime]:
    """
    Convert date strings (e.g., '1/21/2025 2:00:00 PM' or '2025-01-21T14:00:00')
    into a datetime object. Returns None if the format is unrecognized.
    """
    if not dt_str:
        return None
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    logging.warning(f"Unknown date format: '{dt_str}'. Setting to NULL.")
    return None

def process_data():
    """
    Process the downloaded GeoJSON data and load it into the PostgreSQL database.
    
    This function reads the JSON file, extracts data for regions, governorates,
    alerts, hazards, and their relationships, and performs batch upserts into
    the respective tables.
    """
    wait_for_db()
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            logging.info("Loading GeoJSON data from: %s", DEST_FILE)
            with open(DEST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Prepare batch lists.
            region_batch = []
            governorate_batch = []
            alert_batch = []
            hazard_batch = []
            alert_hazard_batch = []
            alert_governorate_batch = []

            # Track seen items to avoid duplicates.
            regions_seen = set()
            govs_seen = set()
            alerts_seen = set()
            hazards_seen = set()

            # Iterate over features.
            features = data.get("features", [])
            for feat in features:
                props = feat.get("properties", {})
                if not props:
                    continue

                # Region info.
                region_id = props.get("Region_ID")
                region_name_ar = props.get("Region_Name_A", "").strip()
                region_name_en = props.get("Region_Name_E", "").strip()

                if region_id and region_id not in regions_seen:
                    regions_seen.add(region_id)
                    region_text = f"{region_name_ar} - {region_name_en}"
                    region_emb = get_embedding(region_text)
                    region_batch.append((region_id, region_name_ar, region_name_en, region_emb))

                # Governorate info.
                gov_id = props.get("GovID")
                gov_name_ar = props.get("Gov_Name_A", "").strip()
                gov_name_en = props.get("Gov_Name_E", "").strip()
                lat = None
                lon = None

                # Attempt to get lat/lon from nested alert governorate data.
                for al in props.get("alert", []):
                    for gsub in al.get("governorates", []):
                        if gsub.get("id") == gov_id:
                            lon = safe_float(gsub.get("longitude"))
                            lat = safe_float(gsub.get("latitude"))
                            break

                if gov_id and gov_id not in govs_seen:
                    govs_seen.add(gov_id)
                    gov_text = f"{gov_name_ar} - {gov_name_en}"
                    gov_emb = get_embedding(gov_text)
                    governorate_batch.append((gov_id, region_id, gov_name_ar, gov_name_en, lat, lon, gov_emb))

                # Alerts.
                for al in props.get("alert", []):
                    alert_id = al.get("id")
                    if not alert_id:
                        continue

                    if alert_id not in alerts_seen:
                        alerts_seen.add(alert_id)
                        from_dt = parse_date(al.get("fromDate"))
                        to_dt = parse_date(al.get("toDate"))
                        alert_batch.append((
                            alert_id,
                            al.get("title", "").strip(),
                            al.get("alertTypeAr", "").strip(),
                            al.get("alertTypeEn", "").strip(),
                            from_dt,
                            to_dt,
                            al.get("alertStatusAr", "").strip(),
                            al.get("alertStatusEn", "").strip()
                        ))

                    if gov_id:
                        alert_governorate_batch.append((alert_id, gov_id))

                    for hz in al.get("alertHazards", []):
                        hz_id = hz.get("id")
                        if not hz_id:
                            continue
                        if hz_id not in hazards_seen:
                            hazards_seen.add(hz_id)
                            desc_ar = hz.get("descriptionAr", "").strip()
                            desc_en = hz.get("descriptionEn", "").strip()
                            combined_hz = f"{desc_ar} | {desc_en}"
                            hz_emb = get_embedding(combined_hz)
                            hazard_batch.append((hz_id, desc_ar, desc_en, hz_emb))
                        alert_hazard_batch.append((alert_id, hz_id))

            # Batched upserts.
            logging.info("Upserting %d regions", len(region_batch))
            if region_batch:
                execute_batch(cur, """
                    INSERT INTO regions (region_id, name_ar, name_en, region_embedding)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (region_id) DO UPDATE
                        SET name_ar = EXCLUDED.name_ar,
                            name_en = EXCLUDED.name_en,
                            region_embedding = EXCLUDED.region_embedding
                """, region_batch, page_size=100)

            logging.info("Upserting %d governorates", len(governorate_batch))
            if governorate_batch:
                execute_batch(cur, """
                    INSERT INTO governorates (gov_id, region_id, name_ar, name_en, latitude, longitude, gov_embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (gov_id) DO UPDATE
                        SET region_id = EXCLUDED.region_id,
                            name_ar   = EXCLUDED.name_ar,
                            name_en   = EXCLUDED.name_en,
                            latitude  = COALESCE(EXCLUDED.latitude, governorates.latitude),
                            longitude = COALESCE(EXCLUDED.longitude, governorates.longitude),
                            gov_embedding = EXCLUDED.gov_embedding
                """, governorate_batch, page_size=100)

            logging.info("Upserting %d alerts", len(alert_batch))
            if alert_batch:
                execute_batch(cur, """
                    INSERT INTO alerts (
                        alert_id, alert_title, alert_type_ar, alert_type_en,
                        from_date, to_date, status_ar, status_en
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (alert_id) DO UPDATE
                        SET alert_title    = EXCLUDED.alert_title,
                            alert_type_ar  = EXCLUDED.alert_type_ar,
                            alert_type_en  = EXCLUDED.alert_type_en,
                            from_date      = EXCLUDED.from_date,
                            to_date        = EXCLUDED.to_date,
                            status_ar      = EXCLUDED.status_ar,
                            status_en      = EXCLUDED.status_en
                """, alert_batch, page_size=100)

            logging.info("Upserting %d hazards", len(hazard_batch))
            if hazard_batch:
                execute_batch(cur, """
                    INSERT INTO hazards (
                        hazard_id, description_ar, description_en, description_embedding
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hazard_id) DO UPDATE
                        SET description_ar        = EXCLUDED.description_ar,
                            description_en        = EXCLUDED.description_en,
                            description_embedding = EXCLUDED.description_embedding
                """, hazard_batch, page_size=100)

            logging.info("Linking %d alert-hazard relationships", len(alert_hazard_batch))
            if alert_hazard_batch:
                execute_batch(cur, """
                    INSERT INTO alert_hazards (alert_id, hazard_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, alert_hazard_batch, page_size=100)

            logging.info("Linking %d alert-governorate relationships", len(alert_governorate_batch))
            if alert_governorate_batch:
                execute_batch(cur, """
                    INSERT INTO alert_governorates (alert_id, gov_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, alert_governorate_batch, page_size=100)

            conn.commit()
            logging.info("Data load completed successfully.")
    except Exception as e:
        conn.rollback()
        logging.error("Main process failed: %s", e)
        raise
    finally:
        conn.close()

def main_loop():
    """
    Main loop that downloads the latest GeoJSON file from the URL specified by
    JSON_DIR, processes the data, and then sleeps for 1 hour before repeating.
    In case of a critical error, it waits 60 seconds before retrying.
    """
    while True:
        try:
            logging.info("=== Starting data load ===")
            download_file(JSON_URL, DEST_FILE, TIMEOUT)
            process_data()
            logging.info("=== Sleeping for 1 hour ===")
            time.sleep(3600)
        except Exception as e:
            logging.error("Critical error: %s", e)
            time.sleep(60)

if __name__ == "__main__":
    main_loop()
