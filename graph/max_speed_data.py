import logging
from typing import Optional, Dict

from log_manager.logger_manager import LoggerManager


class MaxSpeedData:
    """
    Equivalent of the Java MaxSpeedData class.
    Holds forward and reverse max speed values (in km/h).
    """

    def __init__(self, forward: Optional[float] = None, reverse: Optional[float] = None):
        self.forward = forward
        self.reverse = reverse
        # self.logger = LoggerManager(self.__class__.__name__).get_logger()

    @classmethod
    def from_way(cls, way):
        """
        Create MaxSpeedData from an OSM Way object.
        The Way must have a 'tags' attribute (list of Tag objects or dict).
        """
        # Convert tag list (if applicable) to a Python dict
        if isinstance(way.tags, dict):
            tags: Dict[str, str] = way.tags
        else:
            tags = {key: value for key, value in dict(way.tags).items()}

        fwd = None
        rev = None

        # General maxspeed
        maxspeed = tags.get("maxspeed")
        if maxspeed:
            value = cls.parse_value(maxspeed)
            fwd = value
            rev = value

        # Forward-specific maxspeed
        maxspeed_fwd = tags.get("maxspeed:forward")
        if maxspeed_fwd:
            value = cls.parse_value(maxspeed_fwd)
            if value is not None:
                fwd = value

        # Reverse-specific maxspeed
        maxspeed_rev = tags.get("maxspeed:backward")
        if maxspeed_rev:
            value = cls.parse_value(maxspeed_rev)
            if value is not None:
                rev = value

        return cls(fwd, rev)

    @staticmethod
    def parse_value(maxspeed: str) -> Optional[float]:
        """
        Parse the maxspeed value from OSM tag into a float (km/h).
        Supports units: km/h, mph, knots, 'walk', 'none'.
        """
        if not maxspeed:
            return None

        maxspeed = maxspeed.strip().lower()

        if maxspeed == "walk":
            return 15.0
        if maxspeed == "none":
            # 'none' usually means no legal restriction
            return 0.0

        try:
            if maxspeed.endswith("knots"):
                substring = maxspeed[:-5].strip()
                v = float(substring)
                return v * 1.852  # knots → km/h

            elif maxspeed.endswith("mph"):
                substring = maxspeed[:-3].strip()
                v = float(substring)
                return v * 1.609344  # mph → km/h

            elif maxspeed.endswith("km/h"):
                substring = maxspeed[:-4].strip()
                return float(substring)

            else:
                # Default: assume km/h
                return float(maxspeed)

        except ValueError:
            # logger.debug(f"Unable to parse maxspeed '{maxspeed}'")
            return None

    def __repr__(self):
        return f"MaxSpeedData(forward={self.forward}, reverse={self.reverse})"
