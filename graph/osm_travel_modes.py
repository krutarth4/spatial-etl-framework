from enum import Enum


from enum import Enum
from typing import List, Dict, Optional, Set

class OsmTravelModes(Enum):
    """
    Travel modes used in OSM (OpenStreetMap).
    This Python version mirrors the Fraunhofer JOSM-R Java implementation.
    """

    # Basic modes
    DOG = ("dog", [], "walking a dog")
    FOOT = ("foot", ["DOG"], "pedestrian")
    HORSE = ("horse", [], "travel with a horse - this is no vehicle")
    INLINE_SKATES = ("inline_skates", [], "inline skates or roller skates")
    ICE_SKATES = ("ice_skates", [], "ice skates")
    SKI = ("ski", [], "there are also more precise ski:nordic, ski:alpine & ski:telemark but not of interest")
    ELECTRIC_BICYCLE = ("electric_bicycle", [], "riding a bike with electric motor up to approx. 25km/h without license plate required")
    BICYCLE = ("bicycle", ["ELECTRIC_BICYCLE"], "riding a bike")
    KICK_SCOOTER = ("kick_scooter", [], "Non-motorized vehicle to stand on with a handle bar. Accelerated by kicking.")
    CARRIAGE = ("carriage", [], "carriage drawn by horse(s) or other animals")
    CARAVAN = ("caravan", [], "travel trailer, also known as caravan")
    TRAILER = ("trailer", ["CARAVAN"], "needs to be towed by another vehicle which has its own restrictions")
    MOTORCYCLE = ("motorcycle", [], "motorcycle")
    SPEED_PEDELEC = ("speed_pedelec", [], "electric bicycles capable of a higher speed (up to 45 km/h). Requires license, helmet, insurance")
    MOPED = ("moped", ["SPEED_PEDELEC"], "motorized bicycles with speed restriction; max 50cc or 45 km/h")
    MOFA = ("mofa", [], "'low performance moped', max speed of 25 km/h")
    SMALL_ELECTRIC_VEHICLE = ("small_electric_vehicle", [], "Electric scooter - powered by motor, 20-30 km/h")
    MOTORCAR = ("motorcar", [], "automobiles/cars (generic class of double-tracked motorized vehicles)")
    MOTORHOME = ("motorhome", [], "motorhome")
    COACH = ("coach", [], "bus for long-distance travel, not public transport")
    TOURIST_BUS = ("tourist_bus", ["COACH"], "bus for long-distance travel, not public transport")
    GOODS = ("goods", [], "light commercial vehicles up to 3.5 tonnes")
    HGV_ARTICULATED = ("hgv_articulated", [], "articulated heavy goods vehicle")
    BDOUBLE = ("bdouble", [], "EuroCombi up to 60t, see B-train")
    HGV = ("hgv", ["HGV_ARTICULATED", "BDOUBLE"], "heavy goods vehicle > 3.5 tonnes")
    AGRICULTURAL = ("agricultural", [], "agricultural")
    GOLF_CART = ("golf_cart", [], "golf cart")
    BUS = ("bus", [], "a heavy bus acting as a public service vehicle")
    TAXI = ("taxi", [], "taxi")
    MINIBUS = ("minibus", [], "a heavy bus acting as a public service vehicle")
    SHARE_TAXI = ("share_taxi", [], "share taxi")
    PSV = ("psv", ["BUS", "TAXI", "MINIBUS", "SHARE_TAXI"], "public service vehicle")
    CARPOOL = ("carpool", [], "carpool or carpool access")
    HOV = ("hov", ["CARPOOL"], "high-occupancy vehicle/carpool")
    EMERGENCY = ("emergency", [], "emergency motor vehicles (ambulance, fire truck, police)")
    HAZMAT_WATER = ("hazmat:water", [], "vehicles carrying materials which can pollute water")
    HAZMAT = ("hazmat", ["HAZMAT_WATER"], "vehicles carrying hazardous materials")
    DISABLED = ("disabled", [], "vehicles used by disabled persons (often parking)")
    DELIVERY = ("delivery", [], "only when delivering to the element")
    MOTOR_VEHICLE = (
        "motor_vehicle",
        [
            "TRAILER", "MOTORCYCLE", "MOPED", "MOFA", "SMALL_ELECTRIC_VEHICLE", "MOTORCAR",
            "MOTORHOME", "TOURIST_BUS", "GOODS", "HGV", "AGRICULTURAL", "GOLF_CART", "PSV", "HOV",
            "EMERGENCY", "HAZMAT", "DISABLED", "CARPOOL", "DELIVERY"
        ],
        "all motorized vehicle types"
    )
    VEHICLE = ("vehicle", ["BICYCLE", "KICK_SCOOTER", "CARRIAGE", "MOTOR_VEHICLE"], "all vehicle types")
    ACCESS = ("access", ["VEHICLE", "FOOT", "HORSE", "INLINE_SKATES", "ICE_SKATES", "SKI"], "contains all vehicles")

    def __init__(self, key: str, child_names: List[str], description: str):
        self.key = key
        self._child_names = child_names
        self.description = description
        self.fallback_mode: Optional['OsmTravelModes'] = None
        # self.logger = LoggerManager(self.__class__.__name__).get_logger()


    # -------------------------------
    # Lookup and hierarchy functions
    # -------------------------------
    @classmethod
    def get(cls, mode: str) -> Optional['OsmTravelModes']:
        """Get travel mode by name (case-insensitive)."""
        if not mode:
            return None
        for m in cls:
            if m.name.lower() == mode.lower() or m.key.lower() == mode.lower():
                return m
        # .logger.debug(f"Unknown travel mode: '{mode}'")
        return None

    # @classmethod
    # def get_probable(cls, mode: str) -> Optional['OsmTravelModes']:
    #     """Same as get() but without logging debug output."""
    #     for m in cls:
    #         if m.name.lower() == mode.lower() or m.key.lower() == mode.lower():
    #             return m
    #     return None
    #
    # def is_leaf(self) -> bool:
    #     """Return True if this mode has no children."""
    #     return not self._child_names
    #
    # # ------------
    # # Cache fields
    # # ------------
    # # _parent_cache_direct: Dict['OsmTravelModes', List['OsmTravelModes']] = {}
    # # _parent_cache_transitive: Dict['OsmTravelModes', List['OsmTravelModes']] = {}
    # # _children_cache_direct: Dict['OsmTravelModes', List['OsmTravelModes']] = {}
    # # _children_cache_transitive: Dict['OsmTravelModes', List['OsmTravelModes']] = {}
    #
    # # ------------
    # # Relationships
    # # ------------
    # @classmethod
    # def _get_children_internal(cls, mode: 'OsmTravelModes', only_direct: bool) -> List['OsmTravelModes']:
    #     if mode.is_leaf():
    #         return []
    #     result: List[OsmTravelModes] = []
    #     for child_name in mode._child_names:
    #         child = cls.get(child_name)
    #         if not child:
    #             continue
    #         if child not in result:
    #             result.append(child)
    #             if not only_direct:
    #                 result.extend(cls.get_children(child, False))
    #     return result
    #
    # @classmethod
    # def get_children(cls, mode: 'OsmTravelModes', only_direct: bool = True) -> List['OsmTravelModes']:
    #     """Get children of the given mode."""
    #     cache = cls._children_cache_direct if only_direct else cls._children_cache_transitive
    #     if mode in cache:
    #         return cache[mode]
    #     children = cls._get_children_internal(mode, only_direct)
    #     cache[mode] = children
    #     return children
    #
    # @classmethod
    # def _get_parents_internal(cls, mode: 'OsmTravelModes', only_direct: bool) -> List['OsmTravelModes']:
    #     result: List[OsmTravelModes] = []
    #     for potential_parent in cls:
    #         if potential_parent == mode or potential_parent.is_leaf():
    #             continue
    #         if mode.name in potential_parent._child_names:
    #             if potential_parent not in result:
    #                 result.append(potential_parent)
    #                 if not only_direct:
    #                     result.extend(cls.get_parents(potential_parent, False))
    #     return result
    #
    # @classmethod
    # def get_parents(cls, mode: 'OsmTravelModes', only_direct: bool = True) -> List['OsmTravelModes']:
    #     """Get parent modes (more general travel modes)."""
    #     cache = cls._parent_cache_direct if only_direct else cls._parent_cache_transitive
    #     if mode in cache:
    #         return cache[mode]
    #     parents = cls._get_parents_internal(mode, only_direct)
    #     cache[mode] = parents
    #     return parents

    # -----------------
    # Instance methods
    # -----------------
    # def get_parents_instance(self, only_direct: bool = True) -> List['OsmTravelModes']:
    #     """Instance wrapper for get_parents()."""
    #     return self.get_parents(self, only_direct)
    #
    # def get_children_instance(self, only_direct: bool = True) -> List['OsmTravelModes']:
    #     """Instance wrapper for get_children()."""
    #     return self.get_children(self, only_direct)

    def __repr__(self):
        return f"<OsmTravelModes.{self.name}: key={self.key}>"

    # @classmethod
    # def init_caches(cls):
    #     """Preload parent and child caches for all modes."""
    #     # logger.debug("Initializing OsmTravelModes parent/child caches...")
    #     for mode in cls:
    #         cls.get_parents(mode, True)
    #         cls.get_parents(mode, False)
    #         cls.get_children(mode, True)
    #         cls.get_children(mode, False)


# Initialize caches (like Java static block)
# OsmTravelModes.init_caches()

if __name__ == "__main__":


    a = OsmTravelModes.get("bicycle")
    print(a)
    # Example test (similar to Java main)
    # logger.basicConfig(level=logging.INFO)
    # print("Children of VEHICLE:")
    # for c in OsmTravelModes.get_children(OsmTravelModes.VEHICLE, False):
    #     print("  ", c)
    #
    # print("\nParents of BICYCLE:")
    # for p in OsmTravelModes.get_parents(OsmTravelModes.BICYCLE, False):
    #     print("  ", p)



