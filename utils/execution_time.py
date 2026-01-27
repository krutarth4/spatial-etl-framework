import time
import functools

def measure_time(label: str | None = None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.perf_counter() - start

                # Expect first arg to be `self`
                self_obj = args[0] if args else None
                logger = getattr(self_obj, "logger", None)

                name = label or func.__qualname__
                duration = format_duration(duration)

                if logger:
                    logger.critical(f"⏱ {name} executed in {duration}")
                else:
                    print(f"⏱ {name} executed in {duration}")
        return wrapper
    return decorator

def format_duration(seconds: float) -> str:
    ms = int((seconds - int(seconds)) * 1000)
    total_seconds = int(seconds)

    mins, sec = divmod(total_seconds, 60)
    hrs, mins = divmod(mins, 60)

    if hrs > 0:
        return f"{hrs}h {mins}m {sec}s {ms}ms"
    if mins > 0:
        return f"{mins}m {sec}s {ms}ms"
    if sec > 0:
        return f"{sec}s {ms}ms"
    return f"{ms}ms"