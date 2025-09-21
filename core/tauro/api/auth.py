from __future__ import annotations
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import hmac

from tauro.api.config import ApiSettings
from tauro.api.deps import get_settings

_security = HTTPBearer(auto_error=False)


def get_current_user(
    settings: ApiSettings = Depends(get_settings),
    creds: HTTPAuthorizationCredentials | None = Depends(_security),
) -> str:
    if not settings.auth_enabled:
        return "anonymous"
    if not creds or not creds.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token"
        )
    token = creds.credentials
    if not settings.auth_token or not hmac.compare_digest(token, settings.auth_token):
        # mantener mensaje genérico
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token"
        )
    return "api-user"
