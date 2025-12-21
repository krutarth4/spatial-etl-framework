from enum import Enum
from typing import Optional, List, Set, Any

from graph.geo_point import GeoPoint


class Attributes(Enum):
    ROUNDABOUT = ("ROUNDABOUT", "Is a real roundabout.")
    CIRCULAR = ("CIRCULAR", "Similar to a roundabout but not a true one.")
    TUNNEL = ("TUNNEL", "Is a tunnel.")
    BUILDING_PASSAGE = ("BUILDING_PASSAGE", "Tunnel that goes through a building.")
    BRIDGE = ("BRIDGE", "Is a bridge.")
    MOTORROAD = ("MOTORROAD", "Highway with motorway-like restrictions.")
    LIT = ("LIT", "Road lit at night.")
    NOT_LIT = ("NOT_LIT", "Explicitly marked as not lit.")
    CONSTRUCTION = ("CONSTRUCTION", "Road under construction.")
    SIDEPATH = ("SIDEPATH", "Footway or cycleway that is a sidepath.")

    def __init__(self, key: str, description: str):
        self.key = key
        self.description = description

    def __repr__(self):
        return f"{self.key} ({self.description})"

class Link:
    """Numeric representation of travel direction types."""
    NO = 0
    FORWARD = 1
    REVERSE = -1
    BOTH = 2
    Attributes = Attributes

    def __init__(
        self,
        way_id: int,
        way_link_index: int,
        start_node_id: int,
        end_node_id: int,
        geometry: Optional[List["GeoPoint"]] = None,
        highway_type: Optional[Any] = None,
        meters: float = 0.0,
        travel_mode_dots: Optional[Any] = None,
        max_speed_forward: Optional[float] = None,
        max_speed_reverse: Optional[float] = None,
        name: Optional[str] = None,
        attributes: Optional[Set[Attributes]] = None,
        smoothness: Optional[Any] = None,
        surface: Optional[Any] = None,
    ):
        self.way_id = way_id
        self.way_link_index = way_link_index
        self.start_node_id = start_node_id
        self.end_node_id = end_node_id
        self.geometry = geometry or []
        self.highway_type = highway_type
        self.meters = meters
        self.travel_mode_dots = travel_mode_dots
        self.max_speed_forward = max_speed_forward
        self.max_speed_reverse = max_speed_reverse
        self.name = name
        self.attributes = attributes or set()
        self.ATTRIBUTES = attributes or set()
        self.smoothness = smoothness
        self.surface = surface
