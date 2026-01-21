"""
📊 BigData Dashboard - Real Estate Analytics

Chức năng:
- Lambda Architecture: Speed Layer + Batch Layer
- Real-time data visualization (<10s latency)
- Gold analytics từ 4 bảng Spark aggregations
- ML predictions display

Chạy: streamlit run dashboard.py --server.port 8501
URL: http://localhost:8501

Database: PostgreSQL (port 5433)
"""

import streamlit as st
import pandas as pd
import psycopg2
import os
import altair as alt
from datetime import datetime
import warnings
import subprocess
import time

# Silence pandas DBAPI warnings (we intentionally use psycopg2 connections here).
warnings.filterwarnings(
    "ignore",
    message=r"pandas only supports SQLAlchemy connectable.*",
    category=UserWarning,
)

# --- CONFIG ---
st.set_page_config(
    page_title="Trung Tâm Phân Tích Bất Động Sản", 
    layout="wide", 
    page_icon="🏠",
    initial_sidebar_state="expanded"
)

# Professional Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        background-attachment: fixed;
    }
    
    .stApp {
        background: linear-gradient(180deg, #1a1f3c 0%, #232946 50%, #2a3150 100%);
    }
    
    .stApp, .stApp p, .stApp span, .stApp div {
        color: #e8eaf6 !important;
    }
    
    .stMarkdown, .stMarkdown p {
        color: #e8eaf6 !important;
    }
    
    h1, h2, h3, h4, h5, h6 {
        color: #ffffff !important;
    }
    
    label, .stSelectbox label, .stMultiSelect label, .stSlider label {
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
        position: relative;
        overflow: hidden;
    }
    
    .main-header h1 {
        color: white;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 2.5rem;
        margin: 0;
    }
    
    .main-header p {
        color: rgba(255,255,255,0.9);
        font-family: 'Inter', sans-serif;
        font-size: 1.1rem;
        margin-top: 0.5rem;
    }
    
    .metric-card {
        background: linear-gradient(145deg, #2d3250 0%, #3a3f5c 100%);
        border-radius: 16px;
        padding: 1.5rem;
        border: 1px solid rgba(255,255,255,0.15);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        position: relative;
        overflow: hidden;
    }
    
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #667eea, #764ba2);
    }
    
    .metric-icon {
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
    }
    
    .metric-value {
        font-family: 'Inter', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        color: #ffffff !important;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-family: 'Inter', sans-serif;
        font-size: 0.9rem;
        color: #b8c1ec !important;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 500;
    }
    
    .metric-delta {
        font-size: 0.85rem;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        display: inline-block;
        margin-top: 0.5rem;
        font-weight: 600;
    }
    
    .delta-positive {
        background: rgba(0, 200, 83, 0.25);
        color: #69f0ae !important;
    }
    
    .delta-neutral {
        background: rgba(102, 126, 234, 0.25);
        color: #98a8f8 !important;
    }
    
    .section-card {
        background: linear-gradient(145deg, #2d3250 0%, #363b58 100%);
        border-radius: 20px;
        padding: 1.5rem;
        border: 1px solid rgba(255,255,255,0.12);
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
        margin-bottom: 1.5rem;
    }
    
    .section-title {
        font-family: 'Inter', sans-serif;
        font-size: 1.3rem;
        font-weight: 600;
        color: #ffffff !important;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .section-title-icon {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 0.5rem;
        border-radius: 10px;
        font-size: 1.2rem;
    }
    
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #232946 0%, #1e2340 100%);
        border-right: 1px solid rgba(255,255,255,0.1);
    }
    
    .sidebar-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        margin-bottom: 1.5rem;
        text-align: center;
    }
    
    .sidebar-header h2 {
        color: white !important;
        margin: 0;
        font-size: 1.3rem;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        font-weight: 600;
    }
    
    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.5rem 1rem;
        border-radius: 25px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    .status-active {
        background: rgba(0, 200, 83, 0.2);
        color: #69f0ae !important;
        border: 1px solid rgba(0, 200, 83, 0.4);
    }
    
    .pulse-dot {
        width: 8px;
        height: 8px;
        background: #69f0ae;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(1.2); }
    }
    
    .custom-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.5), transparent);
        margin: 2rem 0;
        border: none;
    }
    
    .dashboard-footer {
        text-align: center;
        padding: 1.5rem;
        color: #b8c1ec !important;
        font-size: 0.85rem;
        margin-top: 2rem;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(255,255,255,0.08);
        padding: 0.5rem;
        border-radius: 15px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 0.5rem 1.5rem;
        color: #e8eaf6 !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

# --- DB CONNECTION ---
# K8s: Use service DNS, Local: Use localhost:5433
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "database": os.getenv("POSTGRES_DB", "house_warehouse"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres")
}

# Altair Theme
def configure_altair_theme():
    return {
        'config': {
            'background': 'transparent',
            'view': {'stroke': 'transparent'},
            'axis': {
                'domainColor': 'rgba(255,255,255,0.4)',
                'gridColor': 'rgba(255,255,255,0.15)',
                'labelColor': '#e8eaf6',
                'titleColor': '#ffffff',
                'labelFontSize': 11,
            },
            'legend': {
                'labelColor': '#e8eaf6',
                'titleColor': '#ffffff',
            },
            'title': {
                'color': '#ffffff',
            }
        }
    }

alt.themes.register('dashboard_dark', configure_altair_theme)
alt.themes.enable('dashboard_dark')

@st.cache_data(ttl=0)  # Tắt cache - luôn query DB để có data mới nhất
def load_data():
    conn = psycopg2.connect(**DB_CONFIG)
    
    # SPEED LAYER: Real-time data (latency <10s)
    speed_query = """
    SELECT price, sqft, bedrooms, bathrooms, year_built, location, condition, created_at
    FROM house_data_speed 
    ORDER BY created_at DESC
    """
    
    # BATCH LAYER: High accuracy data
    fact_query = """
    SELECT price, sqft, bedrooms, bathrooms, year_built, location, condition, created_at
    FROM fact_house 
    ORDER BY created_at DESC
    """
    
    # GOLD LAYER: 4 aggregation tables
    loc_query = "SELECT * FROM gold_location_stats ORDER BY avg_price DESC"
    cond_query = "SELECT * FROM gold_condition_stats ORDER BY avg_price DESC"
    bedroom_query = "SELECT * FROM gold_bedroom_analysis ORDER BY bedrooms"
    decade_query = "SELECT * FROM gold_year_built_trends ORDER BY decade"
    
    pred_query = "SELECT actual_price, predicted_price, run_id FROM house_price_predictions ORDER BY as_of_utc DESC LIMIT 500"
    
    # ML METRICS: Read from model metrics table (only r2 and rmse available)
    metrics_query = "SELECT r2, rmse, as_of_utc FROM ml_house_price_model_metrics ORDER BY as_of_utc DESC LIMIT 1"
    
    # Load Speed Layer
    try:
        df_speed = pd.read_sql(speed_query, conn)
        df_speed['created_at'] = pd.to_datetime(df_speed['created_at'])
    except:
        df_speed = pd.DataFrame()
    
    # Load Batch Layer
    try:
        df_fact = pd.read_sql(fact_query, conn)
        if 'created_at' in df_fact.columns:
            df_fact['created_at'] = pd.to_datetime(df_fact['created_at'])
    except:
        df_fact = pd.DataFrame()
        
    # Load Gold Layer
    try:
        df_loc = pd.read_sql(loc_query, conn)
    except:
        df_loc = pd.DataFrame()
    
    try:
        df_cond = pd.read_sql(cond_query, conn)
    except:
        df_cond = pd.DataFrame()
    
    try:
        df_bedroom = pd.read_sql(bedroom_query, conn)
    except:
        df_bedroom = pd.DataFrame()
    
    try:
        df_decade = pd.read_sql(decade_query, conn)
    except:
        df_decade = pd.DataFrame()

    try:
        df_pred = pd.read_sql(pred_query, conn)
    except:
        df_pred = pd.DataFrame()
    
    # Load ML Metrics
    try:
        df_metrics = pd.read_sql(metrics_query, conn)
    except:
        df_metrics = pd.DataFrame()
        
    conn.close()
    return df_speed, df_fact, df_loc, df_cond, df_bedroom, df_decade, df_pred, df_metrics

# --- MAIN APP ---
df_speed, df_fact, df_loc, df_cond, df_bedroom, df_decade, df_pred, df_metrics = load_data()

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("""
    <div class="sidebar-header">
        <h2>🎛️ Bảng Điều Khiển</h2>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="status-badge status-active">
        <div class="pulse-dot"></div>
        Hệ thống hoạt động
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Data Source Selection
    st.markdown("### 🎚️ Nguồn Dữ Liệu")
    data_source = st.radio(
        "Chọn layer",
        ["⚡ Speed Only (Real-time)", "📦 Batch Only (Accurate)"],
        help="Speed: <10s latency | Batch: High accuracy"
    )
    
    if "Speed Only" in data_source:
        df_display = df_speed
    else:
        df_display = df_fact
    
    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
    
    # Filters
    st.markdown("### 🎯 Bộ Lọc")
    all_locations = sorted(df_display['location'].unique()) if not df_display.empty and 'location' in df_display.columns else []
    selected_locs = st.multiselect("📍 Khu vực", all_locations, default=[])
    
    all_conditions = sorted(df_display['condition'].unique()) if not df_display.empty and 'condition' in df_display.columns else []
    selected_conds = st.multiselect("🏷️ Tình trạng nhà", all_conditions, default=[])
    
    if not df_display.empty and 'price' in df_display.columns:
        min_price = int(df_display['price'].min())
        max_price = int(df_display['price'].max())
        price_range = st.slider("💰 Khoảng giá", min_value=min_price, max_value=max_price, value=(min_price, max_price), format="$%d")
    else:
        price_range = (0, 1000000)
    
    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
    
    # Quick Stats
    st.markdown("### 📊 Thống Kê Nhanh")
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        st.metric("⚡ Speed", f"{len(df_speed):,}")
    with col_s2:
        st.metric("📦 Batch", f"{len(df_fact):,}")
    
    if not df_display.empty:
        st.metric("Tổng hiển thị", f"{len(df_display):,}")
        st.metric("Số khu vực", f"{df_display['location'].nunique()}")
        st.metric("Giá TB", f"${df_display['price'].mean():,.0f}")
    
    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
    
    # Actions
    st.markdown("### ⚡ Thao Tác")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Làm mới", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    with col2:
        auto_refresh = st.checkbox("Tự động", value=True)
    
    st.markdown(f"""
    <div style="text-align: center; margin-top: 2rem; color: rgba(255,255,255,0.4); font-size: 0.8rem;">
        Cập nhật lúc: {datetime.now().strftime("%H:%M:%S")}
    </div>
    """, unsafe_allow_html=True)

# Apply Filters
filtered_df = df_display.copy()
if not filtered_df.empty:
    if selected_locs:
        filtered_df = filtered_df[filtered_df['location'].isin(selected_locs)]
    if selected_conds:
        filtered_df = filtered_df[filtered_df['condition'].isin(selected_conds)]
    filtered_df = filtered_df[(filtered_df['price'] >= price_range[0]) & (filtered_df['price'] <= price_range[1])]

# --- HEADER ---
st.markdown("""
<div class="main-header">
    <h1>🏠 Trung Tâm Phân Tích Bất Động Sản</h1>
    <p>Nền tảng Phân tích Thị trường Thời gian thực</p>
</div>
""", unsafe_allow_html=True)

# --- KPI METRICS ---
st.markdown("### 📈 Chỉ Số Hiệu Suất")

k1, k2, k3, k4 = st.columns(4)

# Calculate KPIs from filtered data
if not filtered_df.empty:
    avg_price = filtered_df['price'].mean()
    # Use 'sqft' instead of 'sqft_living' (column name from database)
    avg_sqft = filtered_df['sqft'].mean() if 'sqft' in filtered_df.columns else 0
    total_value = filtered_df['price'].sum()
else:
    avg_price = 0
    avg_sqft = 0
    total_value = 0

with k1:
    speed_count = len(df_speed) if not df_speed.empty else 0
    batch_count = len(df_fact) if not df_fact.empty else 0
    delta_text = f"⚡{speed_count} + 📦{batch_count}" if speed_count > 0 else "📦 Batch only"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">🏘️</div>
        <div class="metric-label">Số BĐS hiện có</div>
        <div class="metric-value">{len(filtered_df):,}</div>
        <div class="metric-delta delta-positive">● {delta_text}</div>
    </div>
    """, unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">💵</div>
            <div class="metric-label">Giá trung bình</div>
            <div class="metric-value">${avg_price:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

    with k3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">📐</div>
            <div class="metric-label">Diện tích TB</div>
            <div class="metric-value">{avg_sqft:,.0f} sqft</div>
        </div>
        """, unsafe_allow_html=True)

    with k4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-icon">💰</div>
            <div class="metric-label">Tổng giá trị</div>
            <div class="metric-value">${total_value/1e6:,.1f}M</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --- SECTION 1.5: REAL-TIME FEED (SPEED LAYER) ---
if not df_speed.empty:
    st.markdown("""
    <div class="section-card">
        <div class="section-title">
            <span class="section-title-icon">⚡</span>
            Luồng Dữ Liệu Real-time (Speed Layer)
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("#### 🔥 Dữ liệu mới nhất")
    df_speed_display = df_speed.head(20)[['created_at', 'location', 'price', 'sqft', 'bedrooms', 'condition']]
    df_speed_display = df_speed_display.rename(columns={
        'created_at': '⏰ Thời gian',
        'location': '📍 Vị trí',
        'price': '💰 Giá',
        'sqft': '📐 DT',
        'bedrooms': '🛏️ PN',
        'condition': '⭐ TT'
    })
    df_speed_display['💰 Giá'] = df_speed_display['💰 Giá'].apply(lambda x: f"${x:,.0f}")
    st.dataframe(df_speed_display, hide_index=True, use_container_width=True, height=300)
    
    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --- SECTION 2: MARKET INTELLIGENCE ---
st.markdown("""
<div class="section-card">
    <div class="section-title">
        <span class="section-title-icon">🗺️</span>
        Bảng Phân Tích Thị Trường
    </div>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📍 Phân tích vùng", "📈 Xu hướng thị trường", "🛏️ Phân tích phòng ngủ", "🏠 Chi tiết BĐS", "⚡ Speed vs Batch"])

with tab1:
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.markdown("#### 🌡️ Bản đồ nhiệt giá theo khu vực")
        if not df_loc.empty:
            chart = alt.Chart(df_loc).mark_bar(cornerRadiusTopRight=8, cornerRadiusBottomRight=8).encode(
                x=alt.X('avg_price:Q', axis=alt.Axis(format='$,d'), title="Giá trung bình ($)"),
                y=alt.Y('location:N', sort='-x', title="Khu vực"),
                color=alt.Color('avg_price:Q', scale=alt.Scale(scheme='viridis')),
                tooltip=[
                    alt.Tooltip('location:N', title='Khu vực'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f'),
                    alt.Tooltip('total_houses:Q', title='Số BĐS'),
                    alt.Tooltip('median_price:Q', title='Giá median', format='$,.0f'),
                    alt.Tooltip('avg_price_per_sqft:Q', title='$/sqft', format='$,.2f'),
                    alt.Tooltip('avg_house_age:Q', title='Tuổi TB', format='.1f')
                ]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("📊 Chưa có dữ liệu vùng. Chạy Spark Gold Job để tạo báo cáo.")
    
    with col_right:
        st.markdown("#### 📊 Phân bố theo khu vực")
        if not df_loc.empty:
            pie_chart = alt.Chart(df_loc).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('total_houses:Q', stack=True),
                color=alt.Color('location:N', scale=alt.Scale(scheme='tableau20')),
                tooltip=[
                    alt.Tooltip('location:N', title='Khu vực'),
                    alt.Tooltip('total_houses:Q', title='Số BĐS'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f')
                ]
            ).properties(height=350)
            st.altair_chart(pie_chart, use_container_width=True)

with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 Xu hướng giá theo thập kỷ xây dựng")
        if not df_decade.empty:
            line = alt.Chart(df_decade).mark_line(strokeWidth=3, point=alt.MarkConfig(size=80, filled=True)).encode(
                x=alt.X('decade:O', title="Thập kỷ xây dựng"),
                y=alt.Y('avg_price:Q', title="Giá trung bình ($)", axis=alt.Axis(format='$,d')),
                tooltip=[
                    alt.Tooltip('decade:O', title='Thập kỷ'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f'),
                    alt.Tooltip('total_houses:Q', title='Số BĐS'),
                    alt.Tooltip('avg_price_per_sqft:Q', title='$/sqft', format='$,.2f'),
                    alt.Tooltip('avg_age:Q', title='Tuổi TB', format='.1f')
                ]
            ).properties(height=350)
            st.altair_chart(line, use_container_width=True)
        else:
            st.info("📈 Đang chờ dữ liệu xu hướng...")
    
    with col2:
        st.markdown("#### 🏷️ Phân tích theo tình trạng (Gold Layer)")
        if not df_cond.empty:
            bars = alt.Chart(df_cond).mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8).encode(
                x=alt.X('condition:N', title="Tình trạng"),
                y=alt.Y('avg_price:Q', title="Giá TB", axis=alt.Axis(format='$,d')),
                color=alt.Color('avg_condition_score:Q', scale=alt.Scale(scheme='plasma'), legend=alt.Legend(title='Điểm TT')),
                tooltip=[
                    alt.Tooltip('condition:N', title='Tình trạng'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f'),
                    alt.Tooltip('total_houses:Q', title='Số BĐS'),
                    alt.Tooltip('median_price:Q', title='Giá median', format='$,.0f'),
                    alt.Tooltip('avg_price_per_sqft:Q', title='$/sqft', format='$,.2f'),
                    alt.Tooltip('avg_condition_score:Q', title='Điểm TT', format='.2f')
                ]
            ).properties(height=350)
            st.altair_chart(bars, use_container_width=True)
        else:
            st.info("🏷️ Đang chờ dữ liệu tình trạng...")

with tab3:
    st.markdown("#### �️ Phân tích giá theo số phòng ngủ")
    
    bed_col1, bed_col2 = st.columns(2)
    
    with bed_col1:
        if not df_bedroom.empty:
            st.markdown("**Giá trung bình theo số phòng ngủ**")
            bedroom_bar = alt.Chart(df_bedroom).mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8).encode(
                x=alt.X('bedrooms:O', title="Số phòng ngủ"),
                y=alt.Y('avg_price:Q', title="Giá TB ($)", axis=alt.Axis(format='$,d')),
                color=alt.Color('avg_price:Q', scale=alt.Scale(scheme='goldred'), legend=None),
                tooltip=[
                    alt.Tooltip('bedrooms:O', title='Phòng ngủ'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f'),
                    alt.Tooltip('total_houses:Q', title='Số BĐS'),
                    alt.Tooltip('median_price:Q', title='Giá median', format='$,.0f'),
                    alt.Tooltip('avg_sqft:Q', title='DT TB', format=',.0f')
                ]
            ).properties(height=350)
            st.altair_chart(bedroom_bar, use_container_width=True)
        else:
            st.info("🛏️ Chưa có dữ liệu phòng ngủ")
    
    with bed_col2:
        if not df_bedroom.empty:
            st.markdown("**Price per Sqft theo phòng ngủ**")
            price_sqft_line = alt.Chart(df_bedroom).mark_line(strokeWidth=3, point=True).encode(
                x=alt.X('bedrooms:O', title="Số phòng ngủ"),
                y=alt.Y('avg_price_per_sqft:Q', title="$/sqft", axis=alt.Axis(format='$,.2f')),
                tooltip=[
                    alt.Tooltip('bedrooms:O', title='Phòng ngủ'),
                    alt.Tooltip('avg_price_per_sqft:Q', title='$/sqft', format='$,.2f'),
                    alt.Tooltip('avg_total_rooms:Q', title='Tổng phòng TB', format='.1f')
                ]
            ).properties(height=350)
            st.altair_chart(price_sqft_line, use_container_width=True)
    
    # Bảng chi tiết
    if not df_bedroom.empty:
        st.markdown("**📊 Bảng thống kê chi tiết**")
        bed_display = df_bedroom[['bedrooms', 'total_houses', 'avg_price', 'median_price', 'min_price', 'max_price', 'avg_sqft', 'avg_price_per_sqft']].copy()
        bed_display.columns = ['🛏️ PN', '🏘️ Số BĐS', '💰 Giá TB', '📊 Median', '⬇️ Min', '⬆️ Max', '📐 DT TB', '💵 $/sqft']
        bed_display['💰 Giá TB'] = bed_display['💰 Giá TB'].apply(lambda x: f"${x:,.0f}")
        bed_display['📊 Median'] = bed_display['📊 Median'].apply(lambda x: f"${x:,.0f}")
        bed_display['⬇️ Min'] = bed_display['⬇️ Min'].apply(lambda x: f"${x:,.0f}")
        bed_display['⬆️ Max'] = bed_display['⬆️ Max'].apply(lambda x: f"${x:,.0f}")
        bed_display['📐 DT TB'] = bed_display['📐 DT TB'].apply(lambda x: f"{x:,.0f}")
        bed_display['💵 $/sqft'] = bed_display['💵 $/sqft'].apply(lambda x: f"${x:.2f}")
        st.dataframe(bed_display, hide_index=True, use_container_width=True)

with tab4:
    st.markdown("#### �🔍 Phân tích chi tiết bất động sản")
    c1, c2 = st.columns([2, 1])
    
    with c1:
        if not filtered_df.empty:
            st.markdown("**Tương quan Giá vs Diện tích** (Màu theo tình trạng)")
            scatter = alt.Chart(filtered_df).mark_circle(opacity=0.7).encode(
                x=alt.X('sqft:Q', title="Diện tích (Sqft)", scale=alt.Scale(zero=False)),
                y=alt.Y('price:Q', title="Giá ($)", axis=alt.Axis(format='$,d')),
                color=alt.Color('condition:N', scale=alt.Scale(scheme='category10')),
                size=alt.Size('bedrooms:Q', scale=alt.Scale(range=[50, 300])),
                tooltip=[
                    alt.Tooltip('location:N', title='Khu vực'),
                    alt.Tooltip('price:Q', title='Giá', format='$,.0f'),
                    alt.Tooltip('sqft:Q', title='Diện tích', format=',.0f'),
                    alt.Tooltip('bedrooms:Q', title='Phòng ngủ'),
                    alt.Tooltip('condition:N', title='Tình trạng')
                ]
            ).interactive().properties(height=450)
            st.altair_chart(scatter, use_container_width=True)
        else:
            st.info("🔍 Không có dữ liệu phù hợp với bộ lọc.")
    
    with c2:
        st.markdown("**📋 Danh sách BĐS mới nhất**")
        if not filtered_df.empty:
            display_df = filtered_df[['location', 'price', 'sqft', 'bedrooms', 'condition']].head(12)
            display_df = display_df.rename(columns={
                'location': '📍 Vị trí', 'price': '💰 Giá', 'sqft': '📐 DT', 'bedrooms': '🛏️ PN', 'condition': '⭐ TT'
            })
            display_df['💰 Giá'] = display_df['💰 Giá'].apply(lambda x: f"${x:,.0f}")
            st.dataframe(display_df, hide_index=True, use_container_width=True, height=400)

with tab5:
    st.markdown("#### ⚡ So Sánh Speed Layer vs Batch Layer")
    
    comp_col1, comp_col2 = st.columns(2)
    
    with comp_col1:
        st.markdown("**🏆 Lambda Architecture Benefits**")
        
        comparison_data = pd.DataFrame({
            'Metric': ['Latency', 'Accuracy', 'Volume', 'Complexity'],
            'Speed Layer': [10, 75, len(df_speed), 'Low'],
            'Batch Layer': [300, 95, len(df_fact), 'High']
        })
        st.dataframe(comparison_data, hide_index=True, use_container_width=True)
        
        st.markdown("""
        **Speed Layer (Real-time):**
        - ✅ Latency <10 giây
        - ✅ Domain-based cleaning
        - ⚠️ No ML predictions
        
        **Batch Layer (High Accuracy):**
        - ✅ Statistical cleaning
        - ✅ ML predictions
        - ⚠️ Latency vài phút
        """)
    
    with comp_col2:
        st.markdown("**📊 Data Distribution**")
        
        if not df_speed.empty or not df_fact.empty:
            source_data = pd.DataFrame({
                'Source': ['Speed Layer', 'Batch Layer'],
                'Count': [len(df_speed), len(df_fact)]
            })
            
            pie = alt.Chart(source_data).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('Count:Q'),
                color=alt.Color('Source:N', scale=alt.Scale(domain=['Speed Layer', 'Batch Layer'], range=['#ff6b6b', '#667eea'])),
                tooltip=[alt.Tooltip('Source:N', title='Source'), alt.Tooltip('Count:Q', title='Records')]
            ).properties(height=250)
            st.altair_chart(pie, use_container_width=True)

# --- SECTION 4: AI VALUATION (ML) ---
st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)

st.markdown("""
<div class="section-card">
    <div class="section-title">
        <span class="section-title-icon">🤖</span>
        Công Cụ Định Giá AI
    </div>
</div>
""", unsafe_allow_html=True)

m1, m2, m3 = st.columns([2, 2, 1])

with m1:
    st.markdown("#### 🎯 Độ chính xác: Giá thực vs Dự đoán")
    if not df_pred.empty:
        base = alt.Chart(df_pred).encode(
            x=alt.X('actual_price:Q', title="Actual Price ($)", axis=alt.Axis(format='$,d')),
            y=alt.Y('predicted_price:Q', title="Predicted Price ($)", axis=alt.Axis(format='$,d'))
        )
        
        points = base.mark_circle(opacity=0.6, size=60).encode(
            color=alt.value('#00c853'),
            tooltip=[
                alt.Tooltip('actual_price:Q', title='Giá thực', format='$,.0f'),
                alt.Tooltip('predicted_price:Q', title='Dự đoán', format='$,.0f')
            ]
        )
        
        min_val = min(df_pred['actual_price'].min(), df_pred['predicted_price'].min())
        max_val = max(df_pred['actual_price'].max(), df_pred['predicted_price'].max())
        line_df = pd.DataFrame({'x': [min_val, max_val], 'y': [min_val, max_val]})
        
        line = alt.Chart(line_df).mark_line(strokeDash=[8, 4], strokeWidth=2, color='#ff6b6b').encode(x='x:Q', y='y:Q')
        
        st.altair_chart((points + line).interactive(), use_container_width=True)
    else:
        st.warning("🤖 Chưa có dữ liệu dự đoán. Chạy Spark ML Inference Job để tạo dự đoán.")

with m2:
    st.markdown("#### 📊 Phân bố sai số dự đoán")
    if not df_pred.empty:
        df_pred_copy = df_pred.copy()
        df_pred_copy['error'] = df_pred_copy['predicted_price'] - df_pred_copy['actual_price']
        
        hist = alt.Chart(df_pred_copy).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, opacity=0.8).encode(
            x=alt.X('error:Q', bin=alt.Bin(maxbins=25), title="Sai số dự đoán ($)"),
            y=alt.Y('count()', title="Tần suất"),
            color=alt.condition(alt.datum.error > 0, alt.value('#ff6b6b'), alt.value('#00c853'))
        ).properties(height=300)
        
        st.altair_chart(hist, use_container_width=True)

with m3:
    st.markdown("#### 📈 Chỉ số mô hình")
    if not df_metrics.empty and not df_pred.empty:
        # Đọc R² và RMSE từ database (bảng ml_house_price_model_metrics)
        r2 = df_metrics['r2'].iloc[0]
        rmse = df_metrics['rmse'].iloc[0]
        
        # Tính MAE và MAPE từ predictions (không có trong metrics table)
        mae = abs(df_pred['predicted_price'] - df_pred['actual_price']).mean()
        mape = (abs(df_pred['predicted_price'] - df_pred['actual_price']) / df_pred['actual_price']).mean() * 100
        
        st.markdown(f"""
        <div class="metric-card" style="margin-bottom: 1rem;">
            <div class="metric-label">MAE</div>
            <div class="metric-value" style="font-size: 1.5rem;">${mae:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card" style="margin-bottom: 1rem;">
            <div class="metric-label">MAPE</div>
            <div class="metric-value" style="font-size: 1.5rem;">{mape:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">R² Score</div>
            <div class="metric-value" style="font-size: 1.5rem;">{r2:.3f}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning("🤖 Chưa có metrics. Chạy Spark ML Training Job.")

# --- FOOTER ---
st.markdown("""
<div class="dashboard-footer">
    <hr class='custom-divider'>
    <p>🏠 Trung Tâm Phân Tích Bất Động Sản VN Brain</p>
    <p style="font-size: 0.75rem;">© 2026 Nền tảng Phân tích Dữ liệu Lớn</p>
</div>
""", unsafe_allow_html=True)

# Auto-refresh logic
if auto_refresh:
    time.sleep(2)  # Giảm xuống 2s để update nhanh hơn
    st.rerun()
