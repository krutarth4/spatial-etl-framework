import functools
import logging


def safe_class(cls):
    """Decorate all public methods to catch/log exceptions using instance logger."""
    for name, func in cls.__dict__.items():
        if isinstance(func, staticmethod):
            continue
        if callable(func) and not name.startswith("_"):

            @functools.wraps(func)
            def wrapper(self, *args, __func=func, __name=name, **kwargs):
                try:
                    return __func(self, *args, **kwargs)
                except Exception as e:
                    logger = getattr(self, "logger", None) or logging.getLogger(cls.__name__)
                    logger.error(
                        f"{cls.__name__}.{__name} failed: {e}",
                        exc_info=True,
                    )
                    if callable(getattr(self, "_note_stage_warning", None)):
                        self._note_stage_warning(__name, e)

            setattr(cls, name, wrapper)

    return cls
