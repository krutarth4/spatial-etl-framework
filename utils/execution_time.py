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

                if logger:
                    logger.info(f"⏱ {name} executed in {duration:.3f}s")
                else:
                    print(f"⏱ {name} executed in {duration:.3f}s")
        return wrapper
    return decorator