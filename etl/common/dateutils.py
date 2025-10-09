from datetime import datetime, timedelta, timezone

def utc_now():
    return datetime.now(timezone.utc)

def last_n_days_window(n_days: int):
    end = utc_now()
    start = end - timedelta(days=n_days)
    # normaliza para meia-noite do start (opcional)
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, end
