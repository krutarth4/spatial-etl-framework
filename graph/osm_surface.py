from enum import Enum
import logging

logger = logging.getLogger(__name__)


class OsmSurface(Enum):
    """
    Enumeration for known types of (road) surfaces.
    https://wiki.openstreetmap.org/wiki/Key:surface
    """

    # --- Paved types ---
    PAVED = ("paved", "Paved.")
    ASPHALT = ("asphalt", "Asphalt.")
    CHIPSEAL = ("chipseal", "Chipseal.")
    CONCRETE = ("concrete", "Concrete.")
    CONCRETE_LANES = ("concrete:lanes", "Concrete - Lanes.")
    CONCRETE_PLATES = ("concrete:plates", "Concrete - Plates.")
    PAVING_STONES = ("paving_stones", "Paving stones.")
    SETT = ("sett", "Sett.")
    UNHEWN_COBBLESTONE = ("unhewn_cobblestone", "Unhewn cobblestone.")
    COBBLESTONE = ("cobblestone", "Cobblestone.")

    # --- Other solid surfaces ---
    BRICKS = ("bricks", "Bricks.")
    METAL = ("metal", "Metal.")
    WOOD = ("wood", "Wood.")
    STEPPING_STONES = ("stepping_stones", "Stepping stones.")
    RUBBER = ("rubber", "Rubber.")

    # --- Unpaved surfaces ---
    UNPAVED = ("unpaved", "Unpaved.")
    COMPACTED = ("compacted", "Compacted.")
    FINE_GRAVEL = ("fine_gravel", "Fine gravel.")
    GRAVEL = ("gravel", "Gravel.")
    SHELLS = ("shells", "Shells.")
    ROCK = ("rock", "Rock.")
    PEBBLESTONE = ("pebblestone", "Pebblestone.")
    GROUND = ("ground", "Ground.")
    DIRT = ("dirt", "Dirt.")
    EARTH = ("earth", "Earth.")
    GRASS = ("grass", "Grass.")
    GRASS_PAVER = ("grass_paver", "Grass paver.")
    METAL_GRID = ("metal_grid", "Metal grid.")
    MUD = ("mud", "Mud.")
    SAND = ("sand", "Sand.")
    WOODCHIPS = ("woodchips", "Woodchips.")
    SNOW = ("snow", "Snow.")
    ICE = ("ice", "Ice.")
    SALT = ("salt", "Salt.")

    # --- Miscellaneous / artificial ---
    CLAY = ("clay", "Clay.")
    TARTAN = ("tartan", "Tartan.")
    ARTIFICIAL_TURF = ("artificial_turf", "Artificial turf.")
    ACRYLIC = ("acrylic", "Acrylic.")
    CARPET = ("carpet", "Carpet.")
    PLASTIC = ("plastic", "Plastic.")

    def __init__(self, key: str, description: str):
        self.key = key
        self.description = description

    # ---------------------------------------------------------
    # Equivalent to: public static OsmSurface get(String key)
    # ---------------------------------------------------------
    @classmethod
    def get(cls, key: str):
        """Get OsmSurface by key string (case-insensitive)."""
        if not key:
            return None
        for surface in cls:
            if surface.key.lower() == key.lower() or surface.name.lower() == key.lower():
                return surface
        logger.debug(f"Unknown 'surface' key: '{key}'")
        return None

    # ---------------------------------------------------------
    # Equivalent to: public static List<String> getKeys()
    # ---------------------------------------------------------
    @classmethod
    def get_keys(cls):
        """Return list of all surface keys."""
        return [surface.key for surface in cls]


# Example usage
if __name__ == "__main__":
    keys = OsmSurface.get_keys()
    print("Available surface keys:", keys)

    print(OsmSurface.get("asphalt"))        # OsmSurface.ASPHALT
    print(OsmSurface.get("very_smooth"))    # None, logs debug
