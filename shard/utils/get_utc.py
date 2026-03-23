from datetime import datetime, timezone
from pydantic import BaseModel, Field

def get_utc_now():
    return datetime.now(timezone.utc)