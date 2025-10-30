import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report
import warnings
import os

# Bỏ qua các cảnh báo không cần thiết
warnings.filterwarnings('ignore')

# --- 1. TẢI DỮ LIỆU TỪ CSV ---
CSV_FILE_PATH = r"D:\bigdata_realtime\rsi_export.csv"
MODEL_SAVE_PATH = "rsi_only_model.xgb"

print(f"Đang tải dữ liệu từ: {CSV_FILE_PATH}...")

if not os.path.exists(CSV_FILE_PATH):
    print(f"Lỗi: Không tìm thấy file tại '{CSV_FILE_PATH}'.")
    print("Vui lòng kiểm tra lại đường dẫn file.")
    exit()

df = pd.read_csv(CSV_FILE_PATH)

# Sắp xếp lại (quan trọng) phòng trường hợp CSV chưa được sắp xếp
df = df.sort_values(by=['symbol', 'close_time']).reset_index(drop=True)

if df.empty:
    print("Không có dữ liệu trong file CSV để huấn luyện.")
    exit()

print(f"Đã tải thành công {len(df)} dòng dữ liệu.")

# --- 2. KỸ THUẬT ĐẶC TRƯNG (FEATURE ENGINEERING) ---
print("Đang tạo features cho RSI...")

# Tạo index thời gian để xử lý (nếu close_time là timestamp ms)
# Nếu 'close_time' đã ở định dạng datetime, bạn có thể bỏ qua dòng này
df['time'] = pd.to_datetime(df['close_time'], unit='ms')
df = df.set_index('time')
df_grouped = df.groupby('symbol')

# Tạo các đặc trưng (features) CHỈ DÙNG RSI
df['rsi_lag_1'] = df_grouped['rsi'].shift(1)
df['rsi_lag_3'] = df_grouped['rsi'].shift(3)
df['rsi_lag_5'] = df_grouped['rsi'].shift(5)
df['rsi_delta_1'] = df['rsi'] - df['rsi_lag_1']

# --- 3. TẠO NHÃN MỤC TIÊU (TARGET LABEL) ---
print("Đang tạo nhãn (target)...")
look_forward_periods = 5  # Dự đoán 5 phút trong tương lai
future_price = df_grouped['close_price'].shift(-look_forward_periods)
df['target'] = (future_price > df['close_price']).astype(int)

# Xóa các hàng bị rỗng (NaN) do dùng shift (ở đầu và cuối)
df = df.dropna()

if df.empty:
    print(f"Dữ liệu quá ít (chỉ {len(df)} dòng) để tạo lag và target.")
    print("Mô hình cần nhiều dữ liệu hơn để huấn luyện.")
    exit()

# --- 4. HUẤN LUYỆN MÔ HÌNH ---
# Xác định các cột feature CHỈ CỦA RSI
feature_columns_rsi = [
    'rsi', 'rsi_lag_1', 'rsi_lag_3', 'rsi_lag_5', 'rsi_delta_1'
]

# Chia 80% train, 20% test theo thời gian (cách làm chuẩn cho time-series)
split_point = int(len(df) * 0.8)
train_df = df.iloc[:split_point]
test_df = df.iloc[split_point:]

X_train = train_df[feature_columns_rsi]
y_train = train_df['target']
X_test = test_df[feature_columns_rsi]
y_test = test_df['target']

if X_train.empty:
    print("Tập huấn luyện (X_train) bị rỗng. Không thể train model.")
    print(f"Dữ liệu của bạn có {len(df)} dòng sau khi làm sạch.")
    print("Hãy thử thu thập thêm dữ liệu (ít nhất vài nghìn dòng).")
    exit()

print(f"Bắt đầu huấn luyện mô hình RSI-Only trên {len(X_train)} dòng...")
print(f"Kiểm thử trên {len(X_test)} dòng.")

model_rsi = xgb.XGBClassifier(
    n_estimators=100,
    learning_rate=0.1,
    max_depth=5,
    use_label_encoder=False,
    eval_metric='logloss'
)
model_rsi.fit(X_train, y_train)
print("Huấn luyện hoàn tất.")

# --- 5. ĐÁNH GIÁ MÔ HÌNH ---
y_pred = model_rsi.predict(X_test)
print("\n--- Báo cáo Phân loại (Mô hình RSI-Only trên Test Set) ---")
print(classification_report(y_test, y_pred, target_names=['Giảm/Giữ', 'Tăng'], zero_division=0))

# --- 6. LƯU MÔ HÌNH ---
model_rsi.save_model(MODEL_SAVE_PATH)
print(f"Đã lưu mô hình thành công vào file: '{MODEL_SAVE_PATH}'")