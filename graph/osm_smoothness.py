from enum import Enum
import logging

logger = logging.getLogger(__name__)


class OsmSmoothness(Enum):
    """
    Smoothness attribute from OSM data:
    https://wiki.openstreetmap.org/wiki/Key:smoothness
    """

    EXCELLENT = ("excellent", "Excellent - Best smoothness.")
    GOOD = ("good", "Good - 2nd best. Not perfect but almost all vehicles can use this smoothness type.")
    INTERMEDIATE = ("intermediate", "Intermediate - Not suitable for small wheels.")
    BAD = ("bad", "Bad - Trekking bike or motorcar.")
    VERY_BAD = ("very_bad", "Very Bad - Car with high clearance.")
    HORRIBLE = ("horrible", "Horrible - Off-road.")
    VERY_HORRIBLE = ("very_horrible", "Very Horrible - Off-road tracks with deep ruts and other obstacles.")
    IMPASSABLE = ("impassable", "Impassable - Not suitable for any wheeled vehicle.")

    def __init__(self, key: str, description: str):
        self.key = key
        self.description = description

    # ------------------------------
    # Equivalent to: public static OsmSmoothness get(String key)
    # ------------------------------
    @classmethod
    def get(cls, key: str):
        """Get OsmSmoothness enum by key string (case-insensitive)."""
        if not key:
            return None
        for item in cls:
            if item.key.lower() == key.lower() or item.name.lower() == key.lower():
                return item
        logger.debug(f"Unknown 'smoothness' key: '{key}'")
        return None

    # ------------------------------
    # Equivalent to: public static List<String> getKeys()
    # ------------------------------
    @classmethod
    def get_keys(cls):
        """Return list of all smoothness keys."""
        return [item.key for item in cls]


# Example usage:
if __name__ == "__main__":
    keys = OsmSmoothness.get_keys()
    print("Smoothness keys:", keys)

    print(OsmSmoothness.get("good"))
    print(OsmSmoothness.get("unknown_value"))  # logs debug
