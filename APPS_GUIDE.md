# 📊 Dashboard & AI Predictor - Hướng Dẫn Sử Dụng

## 🚀 Quick Start

### **Chạy Cả 2 Apps Cùng Lúc (Khuyến Nghị)**

```powershell
.\run_all_apps.ps1
```

**URLs:**
- 📊 **Dashboard Phân Tích:** http://localhost:8501
- 🤖 **AI Dự Đoán Giá:** http://localhost:8502

### **Chạy Từng App Riêng**

```powershell
# Chỉ chạy Dashboard
streamlit run dashboard.py --server.port 8501

# Chỉ chạy AI Predictor
.\run_predictor.ps1
# hoặc: streamlit run predict_app.py --server.port 8502
```

---

## 📊 Dashboard Phân Tích (dashboard.py)

### **Chức Năng:**

✅ **Lambda Architecture:** Merge Speed Layer + Batch Layer
✅ **Real-time Data:** Hiển thị dữ liệu từ Kafka trong <10s
✅ **Gold Analytics:** 4 bảng phân tích từ Spark
✅ **ML Predictions:** Xem kết quả dự đoán từ model

### **Tabs:**

1. **📍 Phân tích vùng** - Giá theo khu vực, heatmap, pie chart
2. **📈 Xu hướng thị trường** - Giá theo thập kỷ, phân tích tình trạng
3. **🛏️ Phân tích phòng ngủ** - Giá theo số phòng ngủ
4. **🏠 Chi tiết BĐS** - Scatter plot, danh sách BĐS
5. **⚡ Speed vs Batch** - So sánh 2 layers

### **Data Sources:**

- **Speed Layer:** `house_data_speed` (real-time từ Spark Streaming)
- **Batch Layer:** `fact_house` (từ Spark Silver job)
- **Gold Layer:** 4 bảng aggregations
  - `gold_location_stats`
  - `gold_condition_stats`
  - `gold_bedroom_analysis`
  - `gold_year_built_trends`
- **ML Layer:** `house_price_predictions`

### **Bộ Lọc (Sidebar):**

- 🎚️ Nguồn dữ liệu: Speed Only / Batch Only / Merge
- 📍 Khu vực
- 🏷️ Tình trạng nhà
- 💰 Khoảng giá

---

## 🤖 AI Dự Đoán Giá (predict_app.py)

### **Chức Năng:**

✅ **Input đơn giản:** Chỉ cần 6 thông tin cơ bản
✅ **Feature Engineering tự động:** Tính 4 features như Spark
✅ **Kết quả chi tiết:** Giá dự đoán + breakdown + confidence interval
✅ **Lịch sử:** Lưu 10 dự đoán gần nhất

### **Cách Dùng:**

**Bước 1:** Nhập thông tin (form bên trái)
- Diện tích (sqft): 100 - 10,000
- Phòng ngủ: 0 - 10
- Phòng tắm: 0 - 10
- Năm xây dựng: 1800 - 2026
- Khu vực: Downtown, Suburb, Rural...
- Tình trạng: Excellent, Good, Fair, Poor

**Bước 2:** Nhấn **"🔮 Dự Đoán Giá"**

**Bước 3:** Xem kết quả (bên phải)
- 💰 Giá dự đoán
- 🔧 Features tự động: house_age, total_rooms, condition_score
- 📊 Breakdown ảnh hưởng từng yếu tố
- 📈 Khoảng tin cậy (±10%)

### **Features Tự Động:**

```python
house_age = 2026 - year_built
total_rooms = bedrooms + bathrooms
condition_score = {'Excellent': 3, 'Good': 2, 'Fair': 1, 'Poor': 0}
price_per_sqft = predicted_price / sqft
```

### **Model:**

- **Hiện tại:** Mock prediction (heuristic-based) - cho kết quả hợp lý để demo
- **Để dùng model thực:** Copy file `house_price_model.pkl` vào thư mục root → app tự động load

---

## 📦 Cài Đặt Dependencies

```powershell
pip install -r requirements.txt
```

Hoặc cài thủ công:

```powershell
pip install streamlit pandas numpy psycopg2-binary altair scikit-learn
```

---

## 🔧 Kết Nối Database

### **PostgreSQL (Dashboard):**

Set environment variables hoặc mặc định:

```powershell
$env:DB_HOST = "localhost"
$env:DB_PORT = "5433"
$env:DB_NAME = "house_warehouse"
$env:DB_USER = "postgres"
$env:DB_PASSWORD = "postgres"
```

### **Port-forward Postgres:**

```powershell
kubectl -n postgres port-forward svc/postgres 5433:5432
```

---

## 🎨 Customization

### **Thêm Location Mới (AI Predictor):**

File: `predict_app.py`, tìm dòng `location = st.selectbox`

```python
location = st.selectbox(
    "📍 Khu vực",
    options=['Downtown', 'Suburb', 'YourNewLocation'],  # Thêm ở đây
)

# Cập nhật giá base
location_multiplier = {
    'Downtown': 400,
    'YourNewLocation': 350,  # Thêm ở đây
}
```

### **Thay Đổi Auto-refresh (Dashboard):**

File: `dashboard.py`, cuối file:

```python
if auto_refresh:
    time.sleep(5)  # Thay đổi giây ở đây
    st.rerun()
```

---

## 🐛 Troubleshooting

### **Port đã được sử dụng:**

```powershell
# Đổi port
streamlit run dashboard.py --server.port 8503
streamlit run predict_app.py --server.port 8504
```

### **Không kết nối được Database:**

```powershell
# Check port-forward đang chạy
kubectl -n postgres get pods
kubectl -n postgres port-forward svc/postgres 5433:5432

# Test kết nối
Test-NetConnection localhost -Port 5433
```

### **Dashboard không có dữ liệu:**

- ✅ Check Spark jobs đã chạy: `kubectl get jobs -n spark`
- ✅ Check PostgreSQL có data: DBeaver → `SELECT COUNT(*) FROM fact_house`
- ✅ Check MinIO có data: http://localhost:9001

### **AI Predictor không chạy:**

- ⚠️ Đang dùng mock prediction - vẫn work bình thường
- 💡 Để dùng model thực, xem phần **Model** ở trên

---

## 📁 File Structure

```
btl_bigdata/
├── dashboard.py              # Dashboard phân tích chính
├── predict_app.py            # AI predictor độc lập
├── run_all_apps.ps1          # Chạy cả 2 apps
├── run_predictor.ps1         # Chạy riêng predictor
├── requirements.txt          # Python dependencies
├── APPS_GUIDE.md            # File này
├── kafka/                    # Kafka components
├── spark/                    # Spark jobs
├── postgres/                 # PostgreSQL configs
└── minio/                    # MinIO configs
```

---

## 📸 Preview

### **Dashboard:**

- KPIs: Số BĐS, giá TB, diện tích TB, tổng giá trị
- Real-time feed: 20 records mới nhất từ Speed Layer
- Charts: Heatmap, pie, bar, line, scatter
- ML Results: Actual vs Predicted

### **AI Predictor:**

- Form input: Clean, validated
- Results card: Large price display
- Features breakdown: 4 cards
- Price analysis: Table breakdown
- History: Last 10 predictions

---

## 🚀 Production Tips

1. **Database Connection Pooling:** Sử dụng `psycopg2.pool` thay vì connect mỗi lần
2. **Cache Strategy:** Tăng TTL nếu data không thay đổi thường xuyên
3. **Load Model Once:** Model đã được cache với `@st.cache_resource`
4. **Environment Variables:** Dùng `.env` file cho production

---

## 📞 Support

- **Pipeline Setup:** Xem [QUICK_START.md](QUICK_START.md)
- **Spark Jobs:** Xem [README.md](README.md)
- **Issues:** Check logs với `kubectl logs` hoặc Streamlit console

---

**Happy Data Analyzing! 📊🤖**
