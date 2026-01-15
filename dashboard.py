import streamlit as st
import pandas as pd
import psycopg2
import time
import os
import altair as alt
from datetime import datetime

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
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global Styles */
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        background-attachment: fixed;
    }
    
    .stApp {
        background: linear-gradient(180deg, #1a1f3c 0%, #232946 50%, #2a3150 100%);
    }
    
    /* Global Text Improvements */
    .stApp, .stApp p, .stApp span, .stApp div {
        color: #e8eaf6 !important;
    }
    
    .stMarkdown, .stMarkdown p {
        color: #e8eaf6 !important;
    }
    
    h1, h2, h3, h4, h5, h6 {
        color: #ffffff !important;
    }
    
    /* Better text contrast for labels */
    label, .stSelectbox label, .stMultiSelect label, .stSlider label {
        color: #ffffff !important;
        font-weight: 500 !important;
    }
    
    /* Hide default Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Custom Header */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem 2.5rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.4);
        position: relative;
        overflow: hidden;
    }
    
    .main-header::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -50%;
        width: 100%;
        height: 200%;
        background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 60%);
    }
    
    .main-header h1 {
        color: white;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 2.5rem;
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    
    .main-header p {
        color: rgba(255,255,255,0.9);
        font-family: 'Inter', sans-serif;
        font-size: 1.1rem;
        margin-top: 0.5rem;
    }
    
    /* Metric Cards */
    .metric-card {
        background: linear-gradient(145deg, #2d3250 0%, #3a3f5c 100%);
        border-radius: 16px;
        padding: 1.5rem;
        border: 1px solid rgba(255,255,255,0.15);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 40px rgba(102, 126, 234, 0.3);
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
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
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
    
    /* Section Cards */
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
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #232946 0%, #1e2340 100%);
        border-right: 1px solid rgba(255,255,255,0.1);
    }
    
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #ffffff !important;
        font-family: 'Inter', sans-serif;
    }
    
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label {
        color: #e8eaf6 !important;
    }
    
    [data-testid="stSidebar"] .stMetric label {
        color: #b8c1ec !important;
    }
    
    [data-testid="stSidebar"] [data-testid="stMetricValue"] {
        color: #ffffff !important;
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
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
    }
    
    /* Data Tables */
    .dataframe {
        border-radius: 10px;
        overflow: hidden;
    }
    
    /* Status Badge */
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
    
    /* Divider */
    .custom-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.5), transparent);
        margin: 2rem 0;
        border: none;
    }
    
    /* Footer */
    .dashboard-footer {
        text-align: center;
        padding: 1.5rem;
        color: #b8c1ec !important;
        font-size: 0.85rem;
        margin-top: 2rem;
    }
    
    .dashboard-footer p {
        color: #b8c1ec !important;
    }
    
    /* Multiselect styling */
    .stMultiSelect {
        background: rgba(255,255,255,0.08);
        border-radius: 10px;
    }
    
    .stMultiSelect [data-baseweb="tag"] {
        background: #667eea !important;
    }
    
    /* Selectbox and Input styling */
    .stSelectbox > div > div,
    .stMultiSelect > div > div {
        background-color: rgba(255,255,255,0.08) !important;
        border-color: rgba(255,255,255,0.2) !important;
        color: #ffffff !important;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(255,255,255,0.08);
        padding: 0.5rem;
        border-radius: 15px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 0.5rem 1.5rem;
        font-family: 'Inter', sans-serif;
        color: #e8eaf6 !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: #ffffff !important;
    }
    
    /* Info boxes */
    .stAlert {
        border-radius: 12px;
        border: none;
        background: rgba(102, 126, 234, 0.15) !important;
        color: #e8eaf6 !important;
    }
    
    .stAlert p {
        color: #e8eaf6 !important;
    }
    
    /* Charts container */
    .chart-container {
        background: rgba(255,255,255,0.03);
        border-radius: 15px;
        padding: 1rem;
        border: 1px solid rgba(255,255,255,0.08);
    }
    
    /* DataFrame styling */
    .stDataFrame {
        background: rgba(255,255,255,0.05);
        border-radius: 10px;
    }
    
    /* Checkbox styling */
    .stCheckbox label span {
        color: #e8eaf6 !important;
    }
    
    /* Slider styling */
    .stSlider [data-baseweb="slider"] {
        background: transparent;
    }
    
    /* Caption styling */
    .stCaption, small {
        color: #b8c1ec !important;
    }
    
    /* Warning box */
    .stWarning {
        background: rgba(255, 193, 7, 0.15) !important;
        color: #ffd54f !important;
    }
    
    .stWarning p {
        color: #ffd54f !important;
    }
</style>
""", unsafe_allow_html=True)

# --- DB CONNECTION ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "database": os.getenv("DB_NAME", "house_warehouse"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres")
}

# Altair Theme Configuration
def configure_altair_theme():
    """Configure a professional dark theme for all Altair charts"""
    return {
        'config': {
            'background': 'transparent',
            'view': {'stroke': 'transparent'},
            'axis': {
                'domainColor': 'rgba(255,255,255,0.4)',
                'gridColor': 'rgba(255,255,255,0.15)',
                'labelColor': '#e8eaf6',
                'titleColor': '#ffffff',
                'tickColor': 'rgba(255,255,255,0.4)',
                'labelFontSize': 11,
                'titleFontSize': 12,
            },
            'legend': {
                'labelColor': '#e8eaf6',
                'titleColor': '#ffffff',
                'labelFontSize': 11,
            },
            'title': {
                'color': '#ffffff',
                'fontSize': 14,
            }
        }
    }

alt.themes.register('dashboard_dark', configure_altair_theme)
alt.themes.enable('dashboard_dark')

@st.cache_data(ttl=5)
def load_data():
    conn = psycopg2.connect(**DB_CONFIG)
    
    # query_fact: Handle potential missing 'created_at' by falling back to ingested_at or simple select
    fact_query = """
    SELECT price, sqft, bedrooms, bathrooms, year_built, location, condition
    FROM fact_house LIMIT 2000
    """
    
    loc_query = "SELECT * FROM gold_location_stats ORDER BY avg_price DESC"
    trend_query = "SELECT * FROM gold_year_trend ORDER BY year_built"
    
    # Try to get predictions if table exists
    pred_query = "SELECT actual_price, predicted_price, run_id FROM house_price_predictions ORDER BY as_of_utc DESC LIMIT 500"
    
    try:
        df_fact = pd.read_sql(fact_query, conn)
    except:
        df_fact = pd.DataFrame()
        
    try:
        df_loc = pd.read_sql(loc_query, conn)
        df_trend = pd.read_sql(trend_query, conn)
    except:
        df_loc, df_trend = pd.DataFrame(), pd.DataFrame()

    try:
        df_pred = pd.read_sql(pred_query, conn)
    except:
        df_pred = pd.DataFrame()
        
    conn.close()
    return df_fact, df_loc, df_trend, df_pred

# --- MAIN APP ---

# Load Data
df_fact, df_loc, df_trend, df_pred = load_data()

# --- SIDEBAR (FILTERS) ---
with st.sidebar:
    # Sidebar Header
    st.markdown("""
    <div class="sidebar-header">
        <h2>🎛️ Bảng Điều Khiển</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # Status indicator
    st.markdown("""
    <div class="status-badge status-active">
        <div class="pulse-dot"></div>
        Hệ thống hoạt động
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Filter Section
    st.markdown("### 🎯 Bộ Lọc")
    
    # Location Filter
    all_locations = sorted(df_fact['location'].unique()) if not df_fact.empty and 'location' in df_fact.columns else []
    selected_locs = st.multiselect(
        "📍 Khu vực", 
        all_locations, 
        default=[],
        help="Lọc bất động sản theo vùng địa lý"
    )
    
    # Condition Filter
    all_conditions = sorted(df_fact['condition'].unique()) if not df_fact.empty and 'condition' in df_fact.columns else []
    selected_conds = st.multiselect(
        "🏷️ Tình trạng nhà", 
        all_conditions, 
        default=[],
        help="Lọc theo tình trạng bất động sản"
    )
    
    # Price Range Filter
    if not df_fact.empty and 'price' in df_fact.columns:
        min_price = int(df_fact['price'].min())
        max_price = int(df_fact['price'].max())
        price_range = st.slider(
            "💰 Khoảng giá",
            min_value=min_price,
            max_value=max_price,
            value=(min_price, max_price),
            format="$%d",
            help="Lọc bất động sản trong khoảng giá"
        )
    else:
        price_range = (0, 1000000)
    
    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
    
    # Quick Stats in Sidebar
    st.markdown("### 📊 Thống Kê Nhanh")
    if not df_fact.empty:
        st.metric("Tổng số BĐS", f"{len(df_fact):,}")
        st.metric("Số khu vực", f"{df_fact['location'].nunique()}")
        st.metric("Giá TB", f"${df_fact['price'].mean():,.0f}")
    
    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
    
    # Actions
    st.markdown("### ⚡ Thao Tác")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Làm mới", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    with col2:
        auto_refresh = st.checkbox("Tự động", value=True, help="Tự động cập nhật mỗi 5 giây")
    
    # Last Updated
    st.markdown(f"""
    <div style="text-align: center; margin-top: 2rem; color: rgba(255,255,255,0.4); font-size: 0.8rem;">
        Cập nhật lúc: {datetime.now().strftime("%H:%M:%S")}
    </div>
    """, unsafe_allow_html=True)

# Apply Filters
filtered_df = df_fact.copy()
if not filtered_df.empty:
    if selected_locs:
        filtered_df = filtered_df[filtered_df['location'].isin(selected_locs)]
    if selected_conds:
        filtered_df = filtered_df[filtered_df['condition'].isin(selected_conds)]
    # Apply price filter
    filtered_df = filtered_df[
        (filtered_df['price'] >= price_range[0]) & 
        (filtered_df['price'] <= price_range[1])
    ]

# --- HEADER SECTION ---
st.markdown("""
<div class="main-header">
    <h1>🏠 Trung Tâm Phân Tích Bất Động Sản</h1>
    <p>Nền tảng Phân tích Thị trường Thời gian thực</p>
</div>
""", unsafe_allow_html=True)

# --- KPI METRICS ROW ---
st.markdown("### 📈 Chỉ Số Hiệu Suất")

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">🏘️</div>
        <div class="metric-label">Số BĐS hiện có</div>
        <div class="metric-value">{len(filtered_df):,}</div>
        <div class="metric-delta delta-positive">● Thời gian thực</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    avg_price = filtered_df['price'].mean() if not filtered_df.empty else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">💵</div>
        <div class="metric-label">Giá trung bình</div>
        <div class="metric-value">${avg_price:,.0f}</div>
        <div class="metric-delta delta-neutral">Giá thị trường</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    avg_sqft = filtered_df['sqft'].mean() if not filtered_df.empty else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">📐</div>
        <div class="metric-label">Diện tích TB</div>
        <div class="metric-value">{avg_sqft:,.0f} sqft</div>
        <div class="metric-delta delta-neutral">Trung bình</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    total_value = filtered_df['price'].sum() if not filtered_df.empty else 0
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon">💰</div>
        <div class="metric-label">Tổng giá trị</div>
        <div class="metric-value">${total_value/1e6:,.1f}M</div>
        <div class="metric-delta delta-positive">● Đang hoạt động</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --- SECTION 2: MARKET INTELLIGENCE (GOLD LAYERS) ---
st.markdown("""
<div class="section-card">
    <div class="section-title">
        <span class="section-title-icon">🗺️</span>
        Bảng Phân Tích Thị Trường
    </div>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["📍 Phân tích vùng", "📈 Xu hướng thị trường", "🏠 Chi tiết BĐS"])

with tab1:
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.markdown("#### 🌡️ Bản đồ nhiệt giá theo khu vực")
        if not df_loc.empty:
            chart = alt.Chart(df_loc).mark_bar(
                cornerRadiusTopRight=8,
                cornerRadiusBottomRight=8
            ).encode(
                x=alt.X('avg_price:Q', axis=alt.Axis(format='$,d'), title="Giá trung bình ($)"),
                y=alt.Y('location:N', sort='-x', title="Khu vực"),
                color=alt.Color('avg_price:Q', 
                    scale=alt.Scale(scheme='viridis'),
                    legend=alt.Legend(title="Giá", format='$,d')
                ),
                tooltip=[
                    alt.Tooltip('location:N', title='Khu vực'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f'),
                    alt.Tooltip('n:Q', title='Số lượng')
                ]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("📊 Chưa có dữ liệu vùng. Chạy Spark Gold Job để tạo báo cáo.")
    
    with col_right:
        st.markdown("#### 📊 Phân bố theo khu vực")
        if not df_loc.empty:
            pie_chart = alt.Chart(df_loc).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('n:Q', stack=True),
                color=alt.Color('location:N', 
                    scale=alt.Scale(scheme='tableau20'),
                    legend=alt.Legend(title="Khu vực")
                ),
                tooltip=[
                    alt.Tooltip('location:N', title='Khu vực'),
                    alt.Tooltip('n:Q', title='Số lượng')
                ]
            ).properties(height=350)
            st.altair_chart(pie_chart, use_container_width=True)

with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 Xu hướng giá theo năm xây dựng")
        if not df_trend.empty:
            line = alt.Chart(df_trend).mark_line(
                strokeWidth=3,
                point=alt.OverlayMarkDef(size=80, filled=True)
            ).encode(
                x=alt.X('year_built:O', title="Năm xây dựng"),
                y=alt.Y('avg_price:Q', title="Giá trung bình ($)", axis=alt.Axis(format='$,d')),
                color=alt.value('#667eea'),
                tooltip=[
                    alt.Tooltip('year_built:O', title='Năm'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f')
                ]
            ).properties(height=350)
            
            # Add gradient area
            area = alt.Chart(df_trend).mark_area(
                opacity=0.3,
                color=alt.Gradient(
                    gradient='linear',
                    stops=[
                        alt.GradientStop(color='#667eea', offset=0),
                        alt.GradientStop(color='transparent', offset=1)
                    ],
                    x1=1, x2=1, y1=1, y2=0
                )
            ).encode(
                x=alt.X('year_built:O'),
                y=alt.Y('avg_price:Q')
            )
            
            st.altair_chart(area + line, use_container_width=True)
        else:
            st.info("📈 Đang chờ dữ liệu xu hướng...")
    
    with col2:
        st.markdown("#### 🏷️ Phân tích theo tình trạng")
        if not filtered_df.empty:
            cond_data = filtered_df.groupby('condition').agg({
                'price': 'mean',
                'sqft': 'count'
            }).reset_index()
            cond_data.columns = ['condition', 'avg_price', 'count']
            
            bars = alt.Chart(cond_data).mark_bar(
                cornerRadiusTopLeft=8,
                cornerRadiusTopRight=8
            ).encode(
                x=alt.X('condition:N', title="Tình trạng"),
                y=alt.Y('avg_price:Q', title="Giá TB", axis=alt.Axis(format='$,d')),
                color=alt.Color('condition:N', scale=alt.Scale(scheme='plasma'), legend=None),
                tooltip=[
                    alt.Tooltip('condition:N', title='Tình trạng'),
                    alt.Tooltip('avg_price:Q', title='Giá TB', format='$,.0f'),
                    alt.Tooltip('count:Q', title='Số lượng')
                ]
            ).properties(height=350)
            st.altair_chart(bars, use_container_width=True)

with tab3:
    st.markdown("#### 🔍 Phân tích chi tiết bất động sản")
    c1, c2 = st.columns([2, 1])
    
    with c1:
        if not filtered_df.empty:
            st.markdown("**Tương quan Giá vs Diện tích** (Màu theo tình trạng)")
            scatter = alt.Chart(filtered_df).mark_circle(
                opacity=0.7,
                stroke='white',
                strokeWidth=1
            ).encode(
                x=alt.X('sqft:Q', title="Diện tích (Sqft)", scale=alt.Scale(zero=False)),
                y=alt.Y('price:Q', title="Giá ($)", axis=alt.Axis(format='$,d')),
                color=alt.Color('condition:N', 
                    scale=alt.Scale(scheme='category10'),
                    legend=alt.Legend(title="Tình trạng", orient='bottom')
                ),
                size=alt.Size('bedrooms:Q', 
                    scale=alt.Scale(range=[50, 300]),
                    legend=alt.Legend(title="Số phòng ngủ")
                ),
                tooltip=[
                    alt.Tooltip('location:N', title='Khu vực'),
                    alt.Tooltip('price:Q', title='Giá', format='$,.0f'),
                    alt.Tooltip('sqft:Q', title='Diện tích', format=',.0f'),
                    alt.Tooltip('bedrooms:Q', title='Phòng ngủ'),
                    alt.Tooltip('bathrooms:Q', title='Phòng tắm'),
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
                'location': '📍 Vị trí',
                'price': '💰 Giá',
                'sqft': '📐 DT',
                'bedrooms': '🛏️ PN',
                'condition': '⭐ TT'
            })
            display_df['💰 Giá'] = display_df['💰 Giá'].apply(lambda x: f"${x:,.0f}")
            display_df['📐 DT'] = display_df['📐 DT'].apply(lambda x: f"{x:,.0f}")
            st.dataframe(
                display_df, 
                hide_index=True, 
                use_container_width=True,
                height=400
            )

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
        # Calculate metrics
        mae = abs(df_pred['predicted_price'] - df_pred['actual_price']).mean()
        mape = (abs(df_pred['predicted_price'] - df_pred['actual_price']) / df_pred['actual_price']).mean() * 100
        
        base = alt.Chart(df_pred).encode(
            x=alt.X('actual_price:Q', title="Actual Price ($)", axis=alt.Axis(format='$,d')),
            y=alt.Y('predicted_price:Q', title="Predicted Price ($)", axis=alt.Axis(format='$,d'))
        )
        
        points = base.mark_circle(
            opacity=0.6,
            size=60
        ).encode(
            color=alt.value('#00c853'),
            tooltip=[
                alt.Tooltip('actual_price:Q', title='Giá thực', format='$,.0f'),
                alt.Tooltip('predicted_price:Q', title='Dự đoán', format='$,.0f')
            ]
        )
        
        # Perfect prediction line
        min_val = min(df_pred['actual_price'].min(), df_pred['predicted_price'].min())
        max_val = max(df_pred['actual_price'].max(), df_pred['predicted_price'].max())
        line_df = pd.DataFrame({'x': [min_val, max_val], 'y': [min_val, max_val]})
        
        line = alt.Chart(line_df).mark_line(
            strokeDash=[8, 4],
            strokeWidth=2,
            color='#ff6b6b'
        ).encode(x='x:Q', y='y:Q')
        
        st.altair_chart((points + line).interactive(), use_container_width=True)
    else:
        st.warning("🤖 Chưa có dữ liệu dự đoán. Chạy Spark ML Inference Job để tạo dự đoán.")

with m2:
    st.markdown("#### 📊 Phân bố sai số dự đoán")
    if not df_pred.empty:
        df_pred['error'] = df_pred['predicted_price'] - df_pred['actual_price']
        df_pred['error_pct'] = ((df_pred['predicted_price'] - df_pred['actual_price']) / df_pred['actual_price']) * 100
        
        hist = alt.Chart(df_pred).mark_bar(
            cornerRadiusTopLeft=4,
            cornerRadiusTopRight=4,
            opacity=0.8
        ).encode(
            x=alt.X('error:Q', 
                bin=alt.Bin(maxbins=25), 
                title="Sai số dự đoán ($)"
            ),
            y=alt.Y('count()', title="Tần suất"),
            color=alt.condition(
                alt.datum.error > 0,
                alt.value('#ff6b6b'),  # Over-predicted
                alt.value('#00c853')   # Under-predicted
            ),
            tooltip=[
                alt.Tooltip('count()', title='Số lượng')
            ]
        ).properties(height=300)
        
        st.altair_chart(hist, use_container_width=True)

with m3:
    st.markdown("#### 📈 Chỉ số mô hình")
    if not df_pred.empty:
        mae = abs(df_pred['predicted_price'] - df_pred['actual_price']).mean()
        mape = (abs(df_pred['predicted_price'] - df_pred['actual_price']) / df_pred['actual_price']).mean() * 100
        r2_approx = 1 - (((df_pred['predicted_price'] - df_pred['actual_price'])**2).sum() / 
                         ((df_pred['actual_price'] - df_pred['actual_price'].mean())**2).sum())
        
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
            <div class="metric-value" style="font-size: 1.5rem;">{r2_approx:.3f}</div>
        </div>
        """, unsafe_allow_html=True)

# --- FOOTER ---
st.markdown("""
<div class="dashboard-footer">
    <hr class='custom-divider'>
    <p>🏠 Trung Tâm Phân Tích Bất Động Sản | Vận hành bởi Spark + Kafka + MinIO + PostgreSQL</p>
    <p style="font-size: 0.75rem;">© 2026 Nền tảng Phân tích Dữ liệu Lớn | Pipeline Dữ liệu Thời gian thực</p>
</div>
""", unsafe_allow_html=True)

# Auto-refresh logic
if auto_refresh:
    time.sleep(5)
    st.rerun()
