from enum import Enum

from graph.osm_travel_modes import OsmTravelModes


class OsmHighwayType(Enum):
    MOTORWAY = ("motorway", {OsmTravelModes.MOTORCAR,
                    OsmTravelModes.MOTORCYCLE,
                    OsmTravelModes.GOODS,
                    OsmTravelModes.HGV,
                    OsmTravelModes.PSV,
                    OsmTravelModes.CARAVAN,
                    OsmTravelModes.TRAILER,
                    OsmTravelModes.MOTORHOME,
                    OsmTravelModes.COACH,
                    OsmTravelModes.TOURIST_BUS,
                    OsmTravelModes.HGV_ARTICULATED,
                    OsmTravelModes.BDOUBLE,
                    OsmTravelModes.BUS,
                    OsmTravelModes.TAXI,
                    OsmTravelModes.MINIBUS,
                    OsmTravelModes.SHARE_TAXI,
                    OsmTravelModes.CARPOOL,
                    OsmTravelModes.HOV,
                    OsmTravelModes.EMERGENCY,
                    OsmTravelModes.HAZMAT_WATER,
                    OsmTravelModes.HAZMAT,
                    OsmTravelModes.DISABLED,
                    OsmTravelModes.DELIVERY}, 130)
    MOTORWAY_LINK = ("motorway_link", {OsmTravelModes.MOTORCAR,
                    OsmTravelModes.MOTORCYCLE,
                    OsmTravelModes.GOODS,
                    OsmTravelModes.HGV,
                    OsmTravelModes.PSV}, 80)
    TRUNK = ("trunk", {OsmTravelModes.ACCESS}, 100)
    TRUNK_LINK = ("trunk_link", {OsmTravelModes.ACCESS}, 80)
    PRIMARY = ("primary", {OsmTravelModes.ACCESS}, 50)
    PRIMARY_LINK = ("primary_link", {OsmTravelModes.ACCESS}, 50)
    SECONDARY = ("secondary", {OsmTravelModes.ACCESS}, 50)
    SECONDARY_LINK = ("secondary_link", {OsmTravelModes.ACCESS}, 50)
    TERTIARY = ("tertiary", {OsmTravelModes.ACCESS}, 50)
    TERTIARY_LINK = ("tertiary_link", {OsmTravelModes.ACCESS}, 50)
    ROAD = ("road", {OsmTravelModes.ACCESS}, 40)
    RESIDENTIAL = ("residential", {OsmTravelModes.ACCESS}, 30)
    LIVING_STREET = ("living_street", {OsmTravelModes.ACCESS}, 15)
    FORD = ("ford", {OsmTravelModes.ACCESS}, 20)
    UNCLASSIFIED = ("unclassified", {OsmTravelModes.ACCESS}, 40)
    SERVICE = ("service", {OsmTravelModes.ACCESS}, 10)
    CYCLEWAY = ("cycleway", {OsmTravelModes.BICYCLE}, 20)
    PATH = ("path", {OsmTravelModes.HORSE, OsmTravelModes.FOOT, OsmTravelModes.BICYCLE, OsmTravelModes.MOFA}, 15)
    TRACK = ("track", {OsmTravelModes.HORSE, OsmTravelModes.FOOT, OsmTravelModes.BICYCLE}, 15)
    FOOTWAY = ("footway", {OsmTravelModes.FOOT}, 15)
    PEDESTRIAN = ("pedestrian", {OsmTravelModes.FOOT}, 5)
    STEPS = ("steps", {OsmTravelModes.FOOT, OsmTravelModes.BICYCLE}, 2)
    CORRIDOR = ("corridor", {OsmTravelModes.FOOT}, 2)
    BRIDLEWAY = ("bridleway", {OsmTravelModes.HORSE}, 30)

    def __init__(self, name, default_modes, default_speed):
        self.osm_name = name
        self.default_modes = default_modes
        self.default_speed = default_speed

    @staticmethod
    def get_all_highway_tags(with_construction=True):
        """Equivalent to getAllHighwayTags(true) in Java."""
        tags = {h.osm_name for h in OsmHighwayType}
        if with_construction:
            tags.add("construction")
        return tags

    @classmethod
    def get(cls, name):
        """
                Look up an OsmHighwayType by name (case-insensitive).
                Logs an error if not found.
                """
        if not name:
            # logger.error("Unable to get highway type for NONE")
            return None

        for member in cls:
            if member.osm_name.lower() == name.lower():
                return member

        # logger.error(f"Unable to get highway type for '{name}'")
        return None
