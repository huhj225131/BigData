import streamlit as st
import pandas as pd
import psycopg2
import time

# Cấu hình kết nối Postgres (khớp với docker-compose)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "house_warehouse",
    "user": "postgres",
    "password": "postgres"
}

st.set_page_config(page_title="Real Estate Monitor", layout="wide")
st.title("🏡 Real Estate Real-time Dashboard")

# Hàm lấy dữ liệu
def load_data():
    conn = psycopg2.connect(**DB_CONFIG)
    # Query lấy dữ liệu mới nhất
    query = """
        SELECT 
            f.price, f.sqft, f.bedrooms, f.bathrooms, f.year_built,
            l.location_name, c.condition_name, f.ingested_at
        FROM fact_house f
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_condition c ON f.condition_id = c.condition_id
        ORDER BY f.ingested_at DESC
        LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Container để auto-refresh
placeholder = st.empty()

while True:
    try:
        df = load_data()
        
        with placeholder.container():
            # 1. Metrics hàng đầu
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("Tổng số căn nhà", len(df))
            
            if not df.empty:
                kpi2.metric("Giá TB ($)", f"{df['price'].mean():,.0f}")
                kpi3.metric("Diện tích TB (sqft)", f"{df['sqft'].mean():,.0f}")
                kpi4.metric("Mới cập nhật", df.iloc[0]['ingested_at'].strftime('%H:%M:%S'))
            
                # 2. Hai biểu đồ song song
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Phân bố giá theo khu vực")
                    # Lấy Top 5 khu vực đắt đỏ nhất
                    top_loc = df.groupby('location_name')['price'].mean().sort_values(ascending=False).head(10)
                    st.bar_chart(top_loc)
                    
                with col2:
                    st.subheader("Tương quan: Giá vs Diện tích")
                    st.scatter_chart(df, x='sqft', y='price', color='condition_name')

                # 3. Bảng dữ liệu chi tiết
                st.subheader("Dữ liệu mới nhất")
                st.dataframe(df.head(10), use_container_width=True)
            else:
                st.warning("Chưa có dữ liệu trong Database. Hãy đợi Producer chạy một chút...")
            
        # Refresh mỗi 3 giây
        time.sleep(3)

    except Exception as e:
        st.error(f"Đang chờ kết nối Database... ({e})")
        time.sleep(5)