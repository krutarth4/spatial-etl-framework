import functools
import inspect


def safe_class(cls):
    """Decorate all public methods to catch/log exceptions using instance logger."""
    for name, func in cls.__dict__.items():
        if callable(func) and not name.startswith("_"):

            @functools.wraps(func)
            def wrapper(self, *args, __func=func, **kwargs):
                try:
                    return __func(self, *args, **kwargs)
                except Exception as e:
                    if hasattr(self, "logger"):
                        self.logger.error(
                            f"{cls.__name__}.{__func.__name__} failed: {e}",
                            exc_info=True
                        )
                    else:
                        self.logger.error(f"{cls.__name__}.{__func.__name__} failed: {e}")


            setattr(cls, name, wrapper)

    return cls