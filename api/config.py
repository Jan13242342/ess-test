import os

# ==================== JWT 鉴权配置 ====================
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "10080"))  # 默认7天

# ==================== 数据库配置 ====================
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://admin:123456@pgbouncer:6432/energy")

# ==================== MQTT 配置 ====================
MQTT_HOST = os.getenv("MQTT_HOST", "emqx")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

# ==================== 固件管理配置 ====================
FIRMWARE_DIR = os.getenv("FIRMWARE_DIR", "./firmware")

# ==================== 设备在线判断配置 ====================
DEVICE_FRESH_SECS = int(os.getenv("DEVICE_FRESH_SECS", "60"))

# ==================== 邮箱验证码配置 ====================
EMAIL_CODE_EXPIRE_MINUTES = 5  # 验证码有效期（分钟）

# ==================== 数据清理配置 ====================
ALARM_HISTORY_RETENTION_DAYS = int(os.getenv("ALARM_HISTORY_RETENTION_DAYS", "90"))
RPC_LOG_RETENTION_DAYS = int(os.getenv("RPC_LOG_RETENTION_DAYS", "30"))