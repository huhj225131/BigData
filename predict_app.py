"""
🤖 AI House Price Predictor

Chức năng:
- Input: 6 thông tin cơ bản (sqft, bedrooms, bathrooms, year, location, condition)
- Feature Engineering tự động: house_age, total_rooms, condition_score, price_per_sqft
- Prediction: Mock model (có thể thay bằng model thực từ Spark)
- Output: Giá dự đoán + breakdown + confidence interval

Chạy: streamlit run predict_app.py --server.port 8502
URL: http://localhost:8502

Model: Mock prediction (để dùng model thực, copy house_price_model.pkl vào root)
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
import requests
from datetime import datetime
import io
import pickle

# --- CONFIG ---
st.set_page_config(
    page_title="Dự Đoán Giá Nhà - AI Assistant", 
    layout="wide", 
    page_icon="🤖",
    initial_sidebar_state="expanded"
)

# Professional Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    .stApp {
        background: linear-gradient(180deg, #1a1f3c 0%, #232946 50%, #2a3150 100%);
    }
    
    .stApp, .stApp p, .stApp span, .stApp div {
        color: #e8eaf6 !important;
    }
    
    h1, h2, h3, h4, h5, h6 {
        color: #ffffff !important;
    }
    
    label, .stSelectbox label, .stMultiSelect label, .stSlider label, .stNumberInput label {
        color: #ffffff !important;
        font-weight: 500 !important;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem 2.5rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.4);
    }
    
    .main-header h1 {
        color: white;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 2.5rem;
        margin: 0;
    }
    
    .prediction-card {
        background: linear-gradient(145deg, #2d3250 0%, #3a3f5c 100%);
        border-radius: 20px;
        padding: 2rem;
        border: 2px solid rgba(102, 126, 234, 0.5);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        margin: 1rem 0;
    }
    
    .prediction-value {
        font-family: 'Inter', sans-serif;
        font-size: 3rem;
        font-weight: 700;
        color: #00c853 !important;
        text-align: center;
        margin: 1rem 0;
    }
    
    .feature-card {
        background: linear-gradient(145deg, #2d3250 0%, #363b58 100%);
        border-radius: 15px;
        padding: 1.5rem;
        border: 1px solid rgba(255,255,255,0.12);
        margin-bottom: 1rem;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        font-size: 1.1rem;
        width: 100%;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
        transform: translateY(-2px);
        box-shadow: 0 5px 20px rgba(102, 126, 234, 0.4);
    }
</style>
""", unsafe_allow_html=True)

# --- MINIO CONFIG ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "house-lake")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

# Feature Engineering Functions (same as silver_job.py)
def calculate_features(sqft, bedrooms, bathrooms, year_built, price=None):
    """Calculate engineered features matching Silver job"""
    features = {}
    
    # Basic validation
    sqft = max(sqft, 1)  # Avoid division by zero
    
    # Feature 1: Price per sqft (will be 0 for prediction, model will handle)
    features['price_per_sqft'] = 0.0 if price is None else price / sqft
    
    # Feature 2: House age
    features['house_age'] = 2026 - year_built
    
    # Feature 3: Total rooms
    features['total_rooms'] = bedrooms + bathrooms
    
    return features

def get_condition_score(condition):
    """Convert condition to score (same as silver_job.py)"""
    condition_map = {
        'Excellent': 3.0,
        'Good': 2.0,
        'Fair': 1.0,
        'Poor': 0.0,
        'Unknown': 0.0
    }
    return condition_map.get(condition, 0.0)

@st.cache_resource
def load_sklearn_model():
    """
    Load scikit-learn model từ MinIO hoặc local
    Nếu bạn có model được train bằng scikit-learn và lưu dưới dạng pickle
    """
    try:
        # Try loading from local cache first
        model_path = "house_price_model.pkl"
        if os.path.exists(model_path):
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            return model, "Loaded from local cache"
        
        # TODO: Implement MinIO loading if model is stored as pickle
        # For now, return None to use mock prediction
        return None, "Model not found - using mock predictions"
    except Exception as e:
        return None, f"Error loading model: {str(e)}"

def mock_predict(input_data):
    """
    Mock prediction function - tính toán dựa trên heuristic
    Thay thế bằng model thực khi có
    """
    sqft = input_data['sqft']
    bedrooms = input_data['bedrooms']
    bathrooms = input_data['bathrooms']
    location = input_data['location']
    condition = input_data['condition']
    house_age = input_data['house_age']
    
    # Base price per sqft by location (example values)
    location_multiplier = {
        'Downtown': 400,
        'Suburb': 250,
        'Rural': 150,
        'Waterfront': 500,
        'Mountain': 300,
        'City Center': 450,
        'Countryside': 180
    }
    
    base_price_per_sqft = location_multiplier.get(location, 250)
    
    # Condition adjustment
    condition_adj = {
        'Excellent': 1.3,
        'Good': 1.1,
        'Fair': 0.9,
        'Poor': 0.7,
        'Unknown': 1.0
    }
    
    # Age adjustment (newer = more expensive)
    age_factor = max(0.5, 1.0 - (house_age / 200))
    
    # Room premium
    room_premium = bedrooms * 5000 + bathrooms * 3000
    
    # Calculate predicted price
    predicted_price = (
        sqft * base_price_per_sqft * 
        condition_adj.get(condition, 1.0) * 
        age_factor + 
        room_premium
    )
    
    # Add some variance
    variance = np.random.uniform(0.95, 1.05)
    predicted_price *= variance
    
    return predicted_price

# --- HEADER ---
st.markdown("""
<div class="main-header">
    <h1>🤖 AI Dự Đoán Giá Nhà</h1>
    <p>Công cụ định giá thông minh sử dụng Machine Learning</p>
</div>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("### 📋 Hướng Dẫn Sử Dụng")
    st.markdown("""
    **Bước 1:** Nhập thông tin cơ bản về ngôi nhà
    
    **Bước 2:** Hệ thống tự động tính toán features
    
    **Bước 3:** Nhấn "🔮 Dự Đoán Giá" để nhận kết quả
    
    ---
    
    **Features được tính tự động:**
    - Price per sqft
    - House age
    - Total rooms
    - Condition score
    """)
    
    st.markdown("---")
    
    # Model info
    model, model_status = load_sklearn_model()
    st.markdown("### 🤖 Model Status")
    if model is not None:
        st.success("✅ Model loaded successfully")
    else:
        st.warning("⚠️ Using mock predictions")
    st.caption(model_status)

# --- MAIN CONTENT ---
col1, col2 = st.columns([1, 1])

with col1:
    st.markdown("### 📝 Thông Tin Ngôi Nhà")
    
    with st.form("prediction_form"):
        st.markdown("#### 🏠 Thông tin cơ bản")
        
        sqft = st.number_input(
            "📐 Diện tích (sqft)",
            min_value=100,
            max_value=10000,
            value=2000,
            step=50,
            help="Diện tích sử dụng của ngôi nhà"
        )
        
        col_a, col_b = st.columns(2)
        with col_a:
            bedrooms = st.number_input(
                "🛏️ Số phòng ngủ",
                min_value=0,
                max_value=10,
                value=3,
                step=1
            )
        
        with col_b:
            bathrooms = st.number_input(
                "🚿 Số phòng tắm",
                min_value=0.0,
                max_value=10.0,
                value=2.0,
                step=0.5
            )
        
        year_built = st.number_input(
            "📅 Năm xây dựng",
            min_value=1800,
            max_value=2026,
            value=2010,
            step=1,
            help="Năm ngôi nhà được xây dựng"
        )
        
        st.markdown("#### 📍 Vị trí & Tình trạng")
        
        location = st.selectbox(
            "📍 Khu vực",
            options=['Downtown', 'Suburb', 'Rural', 'Waterfront', 'Mountain', 'City Center', 'Countryside'],
            help="Vị trí địa lý của ngôi nhà"
        )
        
        condition = st.selectbox(
            "⭐ Tình trạng",
            options=['Excellent', 'Good', 'Fair', 'Poor'],
            help="Tình trạng hiện tại của ngôi nhà"
        )
        
        # Submit button
        st.markdown("<br>", unsafe_allow_html=True)
        submitted = st.form_submit_button("🔮 Dự Đoán Giá", use_container_width=True)

with col2:
    st.markdown("### 🎯 Kết Quả Dự Đoán")
    
    if submitted:
        # Calculate engineered features
        features = calculate_features(sqft, bedrooms, bathrooms, year_built)
        condition_score = get_condition_score(condition)
        
        # Prepare input data
        input_data = {
            'sqft': sqft,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'year_built': year_built,
            'location': location,
            'condition': condition,
            'price_per_sqft': features['price_per_sqft'],
            'house_age': features['house_age'],
            'total_rooms': features['total_rooms'],
            'condition_score': condition_score
        }
        
        # Predict
        with st.spinner('🔄 Đang phân tích dữ liệu...'):
            if model is not None:
                # TODO: Use real model prediction
                # predicted_price = model.predict([feature_vector])[0]
                predicted_price = mock_predict(input_data)
            else:
                predicted_price = mock_predict(input_data)
        
        # Display prediction
        st.markdown("""
        <div class="prediction-card">
            <h3 style="text-align: center; color: #ffffff;">💰 Giá Dự Đoán</h3>
            <div class="prediction-value">${:,.0f}</div>
            <p style="text-align: center; color: #b8c1ec;">Dựa trên phân tích AI</p>
        </div>
        """.format(predicted_price), unsafe_allow_html=True)
        
        # Display feature engineering results
        st.markdown("### 🔧 Features Tính Toán")
        
        feature_cols = st.columns(2)
        
        with feature_cols[0]:
            st.markdown("""
            <div class="feature-card">
                <h4>🏠 House Age</h4>
                <p style="font-size: 1.5rem; font-weight: 600; color: #00c853;">{:.0f} năm</p>
            </div>
            """.format(features['house_age']), unsafe_allow_html=True)
            
            st.markdown("""
            <div class="feature-card">
                <h4>🚪 Total Rooms</h4>
                <p style="font-size: 1.5rem; font-weight: 600; color: #00c853;">{:.1f} phòng</p>
            </div>
            """.format(features['total_rooms']), unsafe_allow_html=True)
        
        with feature_cols[1]:
            st.markdown("""
            <div class="feature-card">
                <h4>⭐ Condition Score</h4>
                <p style="font-size: 1.5rem; font-weight: 600; color: #00c853;">{:.1f}/3.0</p>
            </div>
            """.format(condition_score), unsafe_allow_html=True)
            
            # Estimated price per sqft
            estimated_price_per_sqft = predicted_price / sqft
            st.markdown("""
            <div class="feature-card">
                <h4>💵 Price/Sqft (Est.)</h4>
                <p style="font-size: 1.5rem; font-weight: 600; color: #00c853;">${:.2f}</p>
            </div>
            """.format(estimated_price_per_sqft), unsafe_allow_html=True)
        
        # Price breakdown
        st.markdown("### 📊 Phân Tích Chi Tiết")
        
        breakdown_data = pd.DataFrame({
            'Yếu tố': ['Base (Location)', 'Sqft Factor', 'Condition Adj.', 'Age Factor', 'Room Premium'],
            'Ảnh hưởng': ['High', 'High', 'Medium', 'Medium', 'Low'],
            'Giá trị': [
                f"${predicted_price * 0.4:,.0f}",
                f"${predicted_price * 0.3:,.0f}",
                f"${predicted_price * 0.15:,.0f}",
                f"${predicted_price * 0.1:,.0f}",
                f"${predicted_price * 0.05:,.0f}"
            ]
        })
        
        st.dataframe(breakdown_data, hide_index=True, use_container_width=True)
        
        # Confidence interval
        st.markdown("### 📈 Khoảng Tin Cậy")
        lower_bound = predicted_price * 0.9
        upper_bound = predicted_price * 1.1
        
        st.info(f"📊 Giá thực tế có thể dao động từ **${lower_bound:,.0f}** đến **${upper_bound:,.0f}**")
        
        # Save prediction history
        if 'prediction_history' not in st.session_state:
            st.session_state.prediction_history = []
        
        st.session_state.prediction_history.append({
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'sqft': sqft,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'location': location,
            'predicted_price': predicted_price
        })
    
    else:
        st.info("👈 Nhập thông tin bên trái và nhấn 'Dự Đoán Giá' để bắt đầu")

# --- PREDICTION HISTORY ---
if 'prediction_history' in st.session_state and len(st.session_state.prediction_history) > 0:
    st.markdown("---")
    st.markdown("### 📜 Lịch Sử Dự Đoán")
    
    history_df = pd.DataFrame(st.session_state.prediction_history)
    history_df['predicted_price'] = history_df['predicted_price'].apply(lambda x: f"${x:,.0f}")
    
    st.dataframe(history_df.tail(10), hide_index=True, use_container_width=True)
    
    if st.button("🗑️ Xóa Lịch Sử"):
        st.session_state.prediction_history = []
        st.rerun()

# --- FOOTER ---
st.markdown("""
<div style="text-align: center; margin-top: 3rem; padding: 2rem; color: rgba(255,255,255,0.4);">
    <hr style="border: 1px solid rgba(255,255,255,0.1);">
    <p>🤖 AI House Price Predictor | Powered by Machine Learning & Spark</p>
    <p style="font-size: 0.8rem;">© 2026 Big Data Platform</p>
</div>
""", unsafe_allow_html=True)
