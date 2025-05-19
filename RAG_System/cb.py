import streamlit as st
import requests
from datetime import datetime
import base64
import html
from typing import Dict, Optional

class WeatherApp:
    def __init__(self):
        self.setup_page_config()
        self.load_images()
        self.inject_styles()
        self.initialize_session_state()

    @staticmethod
    def get_image_base64(path: str) -> str:
        """Convert image to base64 string."""
        with open(path, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode()
        return f"data:image/png;base64,{encoded}"

    def load_images(self):
        """Load and store base64 encoded images."""
        self.ncm_logo = self.get_image_base64(r"C:\Users\User\Desktop\Job_Related\NCM_logoB.png")
        self.background = self.get_image_base64(r"C:\Users\User\Desktop\Job_Related\Wb.jpg")

    @staticmethod
    def setup_page_config():
        """Configure Streamlit page settings."""
        st.set_page_config(
            page_title="NCM Early Warning System",
            page_icon="⛈️",
            layout="centered",
            initial_sidebar_state="expanded"
        )

    def inject_styles(self):
        """Inject custom CSS styles."""
        st.markdown(self._get_custom_css(), unsafe_allow_html=True)

    def _get_custom_css(self) -> str:
        """Generate custom CSS styles."""
        return f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
            
            .stApp {{
                background: linear-gradient(rgba(0,0,0,0.4), rgba(0,0,0,0.4)),  /* Darkened overlay */
                           url({self.background});
                background-size: cover;
                background-position: center;
                background-attachment: fixed;
                font-family: 'Inter', sans-serif;
            }}

            .chat-container {{
                background: transparent !important;
                padding: 0.5rem;
                margin: 1rem 0;
                max-height: 68vh;
                overflow-y: auto;
                border-radius: 12px;
            }}

            .chat-container::-webkit-scrollbar {{
                width: 6px;
                background: transparent;
            }}

            .chat-container::-webkit-scrollbar-thumb {{
                background: rgba(0, 70, 127, 0.5);
                border-radius: 3px;
            }}

            .chat-message {{
                padding: 1.5rem;
                margin: 1rem 0;
                border-radius: 16px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.15);
                animation: slideIn 0.3s ease-out;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
            }}

            @keyframes slideIn {{
                from {{ opacity: 0; transform: translateY(20px); }}
                to {{ opacity: 1; transform: translateY(0); }}
            }}

            [data-testid="stMessage"][data-role="assistant"] {{
                background: linear-gradient(135deg, rgba(0,70,127,0.85), rgba(0,96,175,0.85)) !important;
                border-left: none !important;
                color: white !important;
                box-shadow: 0 8px 32px rgba(0,70,127,0.25) !important;
            }}

            [data-testid="stMessage"][data-role="assistant"] code {{
                background: rgba(255,255,255,0.1) !important;
                border: 1px solid rgba(255,255,255,0.2) !important;
                color: #fff !important;
            }}

            [data-testid="stMessage"][data-role="user"] {{
                background: rgba(255,255,255,0.95) !important;
                border-left: none !important;
                box-shadow: 0 8px 32px rgba(0,0,0,0.1) !important;
            }}

            .ncm-header {{
                text-align: center;
                padding: 2.5rem 0;
                background: transparent;
                margin-bottom: 2rem;
                position: relative;
            }}

            .ncm-header::before {{
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: linear-gradient(135deg, rgba(0,70,127,0.85), rgba(0,96,175,0.85));
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
                border-radius: 20px;
                box-shadow: 0 8px 32px rgba(0,70,127,0.25);
                z-index: -1;
            }}

            .ncm-header img {{
                width: 400px;  /* Changed from 260px to 400px */
                margin-bottom: 1.5rem;
                filter: brightness(0) invert(1);
                transition: transform 0.3s ease;
            }}

            .ncm-header img:hover {{
                transform: scale(1.05);
            }}

            .ncm-header h1 {{
                color: white;
                font-size: 2.4rem;
                font-weight: 700;
                margin: 0.5rem 0;
                text-shadow: 0 2px 4px rgba(0,0,0,0.2);
            }}

            .ncm-header p {{
                color: rgba(255,255,255,0.9);
                font-size: 1.1rem;
                margin-top: 0.5rem;
            }}

             /* Modified Sidebar */
            [data-testid="stSidebar"] {{
                background: rgba(0,0,0,0.9) !important;
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
                border-right: 1px solid rgba(255,255,255,0.1);
            }}

            [data-testid="stSidebar"] .block-container {{
                color: white !important;
            }}

            /* Sidebar elements color adjustment */
            [data-testid="stSidebar"] .element-container {{
                color: white !important;
            }}

            [data-testid="stSidebar"] .stSlider label {{
                color: white !important;
            }}

            [data-testid="stSidebar"] .stMarkdown {{
                color: rgba(255,255,255,0.8) !important;
            }}

            .stTextInput>div>div>input {{
                border-radius: 30px !important;
                padding: 1.2rem 1.8rem !important;
                font-size: 1.1rem !important;
                border: 2px solid rgba(0,70,127,0.2) !important;
                background: rgba(255,255,255,0.95) !important;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1) !important;
                transition: all 0.3s ease;
            }}

            .stTextInput>div>div>input:focus {{
                border-color: #00467F !important;
                box-shadow: 0 8px 32px rgba(0,70,127,0.15) !important;
                transform: translateY(-2px);
            }}

            /* Enhanced Response Container */
            .response-container {{
                background: rgba(255,255,255,0.1);
                border-radius: 12px;
                padding: 1.5rem;
                margin: 1rem 0;
                backdrop-filter: blur(8px);
                -webkit-backdrop-filter: blur(8px);
            }}

            .response-list {{
                list-style-type: none;
                padding: 0;
                margin: 0;
            }}

            .response-item {{
                padding: 0.8rem 0;
                border-bottom: 1px solid rgba(255,255,255,0.1);
                color: white;
                font-size: 1.05rem;
                line-height: 1.6;
            }}

            .response-item:last-child {{
                border-bottom: none;
            }}

            /* Hide Streamlit Elements */
            #MainMenu, footer, header {{
                display: none !important;
            }}

            /* Button Styling */
            .stButton>button {{
                border-radius: 25px !important;
                padding: 0.6rem 1.2rem !important;
                background: linear-gradient(135deg, #00467F, #0096FF) !important;
                color: white !important;
                border: none !important;
                box-shadow: 0 4px 15px rgba(0,70,127,0.2) !important;
                transition: all 0.3s ease !important;
            }}

            .stButton>button:hover {{
                transform: translateY(-2px) !important;
                box-shadow: 0 6px 20px rgba(0,70,127,0.3) !important;
            }}

            /* Slider Styling */
            [data-testid="stSlider"] {{
                padding: 1rem 0;
            }}

            .stSlider>div>div>div {{
                background-color: #00467F !important;
            }}
        </style>
        """

    @staticmethod
    def initialize_session_state():
        """Initialize session state variables."""
        if "messages" not in st.session_state:
            st.session_state.messages = []

    @staticmethod
    def process_server_response(response_data: Dict) -> str:
        """Process and format server response."""
        raw_answer = response_data.get("answer", "")
        
        # Clean and deduplicate
        cleaned = html.unescape(raw_answer.replace("```", ""))
        lines = list(dict.fromkeys(line.strip() for line in cleaned.split("\n") if line.strip()))
        
        if "<div" in cleaned:
            return cleaned
        
        # Enhanced HTML formatting
        bullets = "".join(
            f"""<li class='response-item'>
                <div class='response-content'>{line}</div>
               </li>"""
            for line in lines
        )
        
        return f"""
        <div class="response-container">
            <ul class="response-list">
                {bullets}
            </ul>
        </div>
        """

    def render_header(self):
        """Render application header."""
        st.markdown(f"""
        <div class="ncm-header">
            <img src="{self.ncm_logo}" alt="NCM Logo">
            <h1>National Early Warning System</h1>
            <p>Real-time Meteorological Alerts & Predictions</p>
        </div>
        """, unsafe_allow_html=True)

    def render_sidebar(self):
        """Render sidebar configuration."""
        with st.sidebar:
            st.subheader("⚙️ System Configuration")
            st.divider()
            
            k = st.slider("🔍 Search Depth", 1, 10, 5)
            score_threshold = st.slider("🎯 Confidence Threshold", 0.5, 1.0, 0.75)
            
            st.divider()
            if st.button("🔄 Clear Chat History", use_container_width=True):
                st.session_state.messages = []
                st.rerun()
            
            st.divider()
            st.caption(f"System Version: 1.0.0\nLast Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        return k, score_threshold

    def handle_user_input(self, user_input: str, k: int, score_threshold: float):
        """Process user input and get server response."""
        st.session_state.messages.append({"role": "user", "content": user_input})

        with st.spinner("🌩️ Analyzing weather data..."):
            try:
                response = requests.post(
                    "http://localhost:8000/query",
                    json={"query": user_input, "k": k, "score_threshold": score_threshold},
                    timeout=15
                )
                response.raise_for_status()
                server_response = self.process_server_response(response.json())
            except Exception as e:
                server_response = f"⛈️ System error: {str(e)}"

        st.session_state.messages.append({"role": "assistant", "content": server_response})
        st.rerun()

    def render_chat_interface(self):
        """Render chat interface and messages."""
        with st.container():
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            
            for msg in st.session_state.messages:
                with st.chat_message(msg["role"]):
                    st.markdown(msg["content"], unsafe_allow_html=True)
            
            if not st.session_state.messages:
                self.render_welcome_message()
            
            st.markdown('</div>', unsafe_allow_html=True)

    @staticmethod
    def render_welcome_message():
        """Render welcome message for new users."""
        st.markdown("""
        <div style="text-align:center; padding:3rem; margin:2rem 0; border-radius:20px;
                    background:linear-gradient(135deg, rgba(0,70,127,0.85), rgba(0,96,175,0.85));
                    color:white; box-shadow:0 8px 32px rgba(0,70,127,0.25);
                    backdrop-filter:blur(10px); -webkit-backdrop-filter:blur(10px);">
            <div style="font-size:3rem; margin-bottom:1.5rem;">🌤️</div>
            <div style="font-size:1.4rem; font-weight:600; line-height:1.6;">
                Welcome to the Weather Alert System<br>
                <span style="font-size:1.1rem; opacity:0.9;">
                    Enter a location or weather condition to begin
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    def render_footer(self):
        """Render application footer."""
        st.markdown("""
        <div style="text-align:center; color:white; padding:2.5rem; margin-top:3rem;
                    background:linear-gradient(135deg, rgba(0,70,127,0.85), rgba(0,96,175,0.85));
                    border-radius:20px; box-shadow:0 8px 32px rgba(0,70,127,0.25);
                    backdrop-filter:blur(10px); -webkit-backdrop-filter:blur(10px);">
            <div style="font-size:1.2em; font-weight:600; margin-bottom:0.8rem;">
                National Center For Meteorology
            </div>
            <div style="font-size:1rem; opacity:0.9; line-height:1.6;">
                24/7 Meteorological Monitoring • Official Government System<br>
                ⚠️ All warnings should be taken seriously
            </div>
        </div>
        """, unsafe_allow_html=True)

    def run(self):
        """Run the Streamlit application."""
        self.render_header()
        k, score_threshold = self.render_sidebar()
        self.render_chat_interface()
        
        user_input = st.chat_input("Enter your meteorological query...")
        if user_input:
            self.handle_user_input(user_input, k, score_threshold)
        
        self.render_footer()

if __name__ == "__main__":
    app = WeatherApp()
    app.run()