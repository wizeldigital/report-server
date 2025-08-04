from fastapi import HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.requests import Request
from typing import Optional
from ..core.config import settings

security = HTTPBearer(auto_error=False)


async def validate_private_key(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security)
) -> bool:
    """
    Validate private key from various sources:
    - x-private-key header
    - Authorization header (Bearer token)
    - Query parameter privateKey
    - Request body privateKey
    """
    private_key = None
    
    # Check x-private-key header
    private_key = request.headers.get("x-private-key")
    
    # Check Authorization header
    if not private_key and credentials:
        private_key = credentials.credentials
    
    # Check query parameter
    if not private_key:
        private_key = request.query_params.get("privateKey")
    
    # Check request body (for POST/PUT requests)
    if not private_key and request.method in ["POST", "PUT", "PATCH"]:
        try:
            body = await request.json()
            private_key = body.get("privateKey")
        except Exception:
            pass
    
    if not private_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Private key is required"
        )
    
    if private_key != settings.PRIVATE_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid private key"
        )
    
    return True