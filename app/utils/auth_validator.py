"""
Enhanced authentication validator for Otoshi Launcher
"""

"""Auth helpers.

This project already depends on `python-jose` (see requirements.txt).
The previous implementation attempted to import `PyJWT`, which is not
declared as a dependency and caused the backend dev server to crash.
"""

from jose import jwt
from jose.exceptions import ExpiredSignatureError, JWTError
import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from functools import wraps

from ..core.config import SECRET_KEY, ALGORITHM
from ..core.cache import cache_client


class AuthValidator:
    """Enhanced authentication validator with strict checks"""
    
    @staticmethod
    def validate_token_strict(token: str) -> Dict[str, Any]:
        """
        Strictly validate JWT token with comprehensive checks
        
        Args:
            token: JWT access token
            
        Returns:
            Dict with validation result and user info
            
        Raises:
            ValueError: If token is invalid
        """
        if not token or not isinstance(token, str):
            raise ValueError("Token is required and must be a string")
        
        token = token.strip()
        if not token:
            raise ValueError("Token cannot be empty")
        
        try:
            # Decode and validate JWT
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            
            # Check token type
            token_type = payload.get("type")
            if token_type not in (None, "access"):
                raise ValueError("Invalid token type")
            
            # Extract user ID
            user_id = payload.get("sub")
            if not user_id:
                raise ValueError("Token missing user ID")
            
            # Check expiration
            exp = payload.get("exp")
            if not exp:
                raise ValueError("Token missing expiration")
            
            if time.time() > exp:
                raise ValueError("Token has expired")
            
            # Check if token is close to expiry (< 5 minutes)
            time_to_expiry = exp - time.time()
            needs_refresh = time_to_expiry < 300  # 5 minutes
            
            # Validate against active session
            session_token = cache_client.get_session(user_id)
            if session_token and session_token != token:
                raise ValueError("Token session has been invalidated")
            
            return {
                "valid": True,
                "user_id": user_id,
                "expires_at": exp,
                "needs_refresh": needs_refresh,
                "time_to_expiry": time_to_expiry,
                "payload": payload
            }
            
        except ExpiredSignatureError:
            raise ValueError("Token has expired")
        except JWTError as e:
            raise ValueError(f"Invalid token: {str(e)}")
        except Exception as e:
            raise ValueError(f"Token validation failed: {str(e)}")


def check_download_permission(user_id: str, game_id: str) -> bool:
    """
    Check if user has permission to download a specific game
    
    Args:
        user_id: User ID from validated token
        game_id: Game ID to download
        
    Returns:
        bool: True if user can download, False otherwise
    """
    # Check if user owns the game (implement your logic here)
    # For now, return True for all authenticated users
    # In production, check against user's library/purchases
    
    return True


def log_download_attempt(user_id: str, game_id: str, success: bool, error: str = None):
    """
    Log download attempts for security monitoring
    
    Args:
        user_id: User ID
        game_id: Game ID
        success: Whether download started successfully
        error: Error message if failed
    """
    timestamp = datetime.utcnow().isoformat()
    
    log_entry = {
        "timestamp": timestamp,
        "user_id": user_id,
        "game_id": game_id,
        "action": "download_attempt",
        "success": success,
        "error": error
    }
    
    # Store in cache for monitoring (expire after 24 hours).
    # Keep logging non-blocking: failures here must never break download flow.
    try:
        cache_key = f"download_log:{str(user_id)}:{timestamp}"
        cache_client.set_json(cache_key, log_entry, ttl=86400)
    except Exception:
        pass
    
    # Also log to file/database in production
    print(f"[DOWNLOAD_LOG] {log_entry}")


# Convenience functions
def validate_user_token(token: str) -> Optional[str]:
    """
    Validate token and return user_id if valid
    
    Args:
        token: JWT token
        
    Returns:
        str: user_id if valid, None if invalid
    """
    try:
        result = AuthValidator.validate_token_strict(token)
        return result["user_id"]
    except ValueError:
        return None


def is_token_expired(token: str) -> bool:
    """
    Check if token is expired
    
    Args:
        token: JWT token
        
    Returns:
        bool: True if expired, False if valid
    """
    try:
        AuthValidator.validate_token_strict(token)
        return False
    except ValueError as e:
        return "expired" in str(e).lower()
