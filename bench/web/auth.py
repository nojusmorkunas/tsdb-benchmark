import base64
import os
import secrets

from fastapi import Request
from fastapi.responses import Response

APP_USER = os.environ.get("APP_USER", "admin")
APP_PASS = os.environ.get("APP_PASS", "")
_SESSION_TOKEN = secrets.token_hex(16)  # random per restart; cookie lets browser fetch() skip re-auth


async def basic_auth_middleware(request: Request, call_next):
    # Valid session cookie → allow (covers browser fetch() calls after initial page load)
    if request.cookies.get("bench_sid") == _SESSION_TOKEN:
        return await call_next(request)
    # Valid Basic auth → allow and set session cookie
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Basic "):
        try:
            decoded = base64.b64decode(auth[6:]).decode()
            username, password = decoded.split(":", 1)
            if (secrets.compare_digest(username, APP_USER) and
                    secrets.compare_digest(password, APP_PASS)):
                resp = await call_next(request)
                resp.set_cookie("bench_sid", _SESSION_TOKEN, max_age=86400,
                                httponly=True, samesite="strict")
                return resp
        except Exception:
            pass
    return Response(
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="DB Benchmark"'},
        content="Unauthorized",
    )
