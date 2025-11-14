import bcrypt
import jwt
from datetime import datetime, timezone, timedelta
from random import randint
from typing import Dict
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from main import engine
from config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, EMAIL_CODE_EXPIRE_MINUTES

router = APIRouter(prefix="/api/v1", tags=["鉴权 | Authentication"])

security = HTTPBearer()
# SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "10080"))  # 默认7天

# ==================== 模型定义 ====================

class EmailCodeRequest(BaseModel):
    email: EmailStr

class RegisterRequest(BaseModel):
    email: EmailStr
    code: str
    password: str
    username: str

class LoginRequest(BaseModel):
    email_or_username: str  # 兼容邮箱或用户名
    password: str    # ← 必须是字符串

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: int
    username: str
    role: str

# ==================== 发送注册验证码 ====================

@router.post(
    "/send_email_code_register",
    summary="发送注册验证码 | Send Register Code",
    description="""
**权限要求 | Required Role**: 无需登录 | No login required

向指定邮箱发送注册验证码,验证码5分钟内有效。

Send registration verification code to the specified email, valid for 5 minutes.

📝 **注意 | Note**:
- 邮箱不能已注册 | Email must not be registered
- 验证码5分钟有效 | Code valid for 5 minutes
- 测试环境会返回验证码 | Test environment returns code directly
"""
)
async def send_email_code_register(data: EmailCodeRequest):
    # 检查邮箱是否已注册
    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT 1 FROM users WHERE email=:email"),
            {"email": data.email}
        )
        if result.first():
            raise HTTPException(
                status_code=400,
                detail={"msg": "该邮箱已注册", "msg_en": "This email is already registered"}
            )
    
    # 生成6位验证码
    code = f"{randint(100000, 999999)}"
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=EMAIL_CODE_EXPIRE_MINUTES)
    
    # 写入数据库
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO email_codes (email, code, purpose, expires_at)
                VALUES (:email, :code, :purpose, :expires_at)
            """),
            {
                "email": data.email,
                "code": code,
                "purpose": "register",
                "expires_at": expires_at
            }
        )
    
    # 测试环境直接返回验证码（生产环境应发送邮件）
    return {
        "msg": "验证码已生成（测试环境直接返回）",
        "msg_en": "Verification code generated (returned for testing)",
        "code": code  # 生产环境请删除此行
    }

# ==================== 用户注册 ====================

@router.post(
    "/register",
    summary="用户注册 | User Register",
    description="""
**权限要求 | Required Role**: 无需登录 | No login required

使用邮箱验证码注册新用户。

Register new user with email verification code.

📝 **注意 | Note**:
- 需先调用发送验证码接口 | Must call send code API first
- 验证码5分钟有效 | Code valid for 5 minutes
- 默认角色为 user | Default role is user
"""
)
async def register(data: RegisterRequest):
    async with engine.begin() as conn:
        # 验证邮箱验证码
        code_row = (await conn.execute(
            text("""
                SELECT code, expires_at, used
                FROM email_codes
                WHERE email=:email AND purpose='register'
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"email": data.email}
        )).mappings().first()
        
        if not code_row:
            raise HTTPException(status_code=400, detail="验证码不存在")
        if code_row["used"]:
            raise HTTPException(status_code=400, detail="验证码已使用")
        if code_row["expires_at"] < datetime.now(timezone.utc):
            raise HTTPException(status_code=400, detail="验证码已过期")
        if code_row["code"] != data.code:
            raise HTTPException(status_code=400, detail="验证码错误")
        
        # 检查邮箱是否已注册
        exists = (await conn.execute(
            text("SELECT 1 FROM users WHERE email=:email"),
            {"email": data.email}
        )).first()
        if exists:
            raise HTTPException(status_code=400, detail="邮箱已注册")
        
        # 创建用户
        hashed = bcrypt.hashpw(data.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        await conn.execute(
            text("""
                INSERT INTO users (username, email, password_hash, role)
                VALUES (:username, :email, :password_hash, 'user')
            """),
            {
                "username": data.username,
                "email": data.email,
                "password_hash": hashed
            }
        )
        
        # 标记验证码已使用
        await conn.execute(
            text("""
                UPDATE email_codes
                SET used=true
                WHERE email=:email AND purpose='register' AND code=:code
            """),
            {"email": data.email, "code": data.code}
        )
    
    return {"msg": "注册成功", "msg_en": "Registration successful"}

# ==================== 用户登录 ====================

@router.post(
    "/login",
    response_model=LoginResponse,
    summary="用户登录 | User Login",
    description="""
**权限要求 | Required Role**: 无需登录 | No login required

使用邮箱和密码登录，返回 JWT Token。

Login with email and password, returns JWT Token.

📝 **注意 | Note**:
- Token 默认有效期7天 | Token valid for 7 days by default
- 请在请求头中携带: Authorization: Bearer {token}
"""
)
async def login(data: LoginRequest):
    # 查询时同时匹配邮箱和用户名
    async with engine.connect() as conn:
        user_row = (await conn.execute(
            text("""
                SELECT id, username, email, password_hash, role 
                FROM users 
                WHERE email=:input OR username=:input
            """),
            {"input": data.email_or_username}
        )).mappings().first()
        
        if not user_row:
            raise HTTPException(
                status_code=401,
                detail={"msg": "邮箱或密码错误", "msg_en": "Invalid email or password"}
            )
        
        if not bcrypt.checkpw(data.password.encode("utf-8"), user_row["password_hash"].encode("utf-8")):
            raise HTTPException(status_code=401, detail="邮箱或密码错误")
    
    # 生成 JWT Token
    payload = {
        "user_id": user_row["id"],
        "username": user_row["username"],
        "role": user_row["role"],
        "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user_id": user_row["id"],
        "username": user_row["username"],
        "role": user_row["role"]
    }

# ==================== 获取当前用户信息 ====================

@router.get(
    "/getinfo",
    summary="获取当前用户信息 | Get Current User Info",
    description="""
**权限要求 | Required Role**: 所有已登录用户 | All logged-in users

返回当前登录用户的基本信息。

Return basic info of the current logged-in user.

📝 **注意 | Note**:
- 需在请求头携带有效 Token | Valid token required in header
"""
)
async def get_info(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
        
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT username, email, role FROM users WHERE id=:uid"),
                {"uid": user_id}
            )
            row = result.first()
            if not row:
                raise HTTPException(status_code=404, detail="用户不存在")
            info = row._mapping
            return {
                "username": info["username"],
                "email": info["email"],
                "role": info["role"]
            }
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token已过期")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="无效的Token")

# ==================== 用户登出 ====================

@router.post(
    "/logout",
    summary="用户登出 | User Logout",
    description="""
**权限要求 | Required Role**: 所有已登录用户 | All logged-in users

前端调用后应删除本地JWT令牌，后端不做实际操作。

Frontend should delete the local JWT token after calling this API.

📝 **注意 | Note**:
- JWT 是无状态的，后端无法主动失效 | JWT is stateless, backend cannot invalidate
- 前端需自行删除本地 Token | Frontend must delete local token
"""
)
async def logout():
    return {"msg": "登出成功", "msg_en": "Logout success"}