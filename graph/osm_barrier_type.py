from enum import Enum
from typing import Set
from dataclasses import dataclass

from graph.osm_travel_modes import OsmTravelModes


class OsmBarrierType(Enum):
    BLOCK = ("block", set(), set())
    BOLLARD = (
        "bollard",
        {OsmTravelModes.FOOT, OsmTravelModes.BICYCLE, OsmTravelModes.HORSE, OsmTravelModes.MOFA, OsmTravelModes.MOPED},
        {OsmTravelModes.ACCESS},
    )
    BORDER_CONTROL = ("border_control", set(), set())
    BUMP_GATE = ("bump_gate", set(), set())
    BUS_TRAP = ("bus_trap", set(), set())
    CATTLE_GRID = ("cattle_grid", set(), set())
    COUPURE = ("coupure", set(), set())
    CYCLE_BARRIER = ("cycle_barrier", set(), set())
    DEBRIS = ("debris", set(), set())
    ENTRANCE = ("entrance", set(), set())
    FULL_HEIGHT_TURNSTILE = ("full-height_turnstile", set(), set())
    GATE = ("gate", set(), set())
    HAMPSHIRE_GATE = ("hampshire_gate", set(), set())
    HEIGHT_RESTRICTOR = ("height_restrictor", set(), set())
    HORSE_STILE = ("horse_stile", set(), set())
    KENT_CARRIAGE_GAP = ("kent_carriage_gap", set(), set())
    KISSING_GATE = ("kissing_gate", set(), set())
    LIFT_GATE = ("lift_gate", set(), set())
    MOTORCYCLE_BARRIER = ("motorcycle_barrier", set(), set())
    PLANTER = ("planter", set(), set())
    SALLY_PORT = ("sally_port", set(), set())
    SLIDING_BEAM = ("sliding_beam", set(), set())
    SLIDING_GATE = ("sliding_gate", set(), set())
    SPIKES = ("spikes", set(), set())
    STILE = ("stile", set(), set())
    SUMP_BUSTER = ("sump_buster", set(), set())
    SWING_GATE = ("swing_gate", set(), set())
    TOLL_BOOTH = ("toll_booth", set(), set())
    TURNSTILE = ("turnstile", set(), set())
    WEDGE = ("wedge", set(), set())
    WICKET_GATE = ("wicket_gate", set(), set())
    YES = ("yes", set(), set())
    BAR = ("bar", set(), set())
    BARRIER_BOARD = ("barrier_board", set(), set())
    CHAIN = ("chain", set(), set())
    JERSEY_BARRIER = ("jersey_barrier", set(), set())
    KERB = ("kerb", {OsmTravelModes.ACCESS}, set())
    LOG = ("log", set(), set())
    ROPE = ("rope", set(), set())
    TANK_TRAP = ("tank_trap", set(), set())
    TYRES = ("tyres", set(), set())

    def __init__(self, osm_name: str, modes_allowed_implicit: Set[OsmTravelModes], modes_restricted_implicit: Set[OsmTravelModes]):
        self.osm_name = osm_name
        self.modes_allowed_implicit = modes_allowed_implicit
        self.modes_restricted_implicit = modes_restricted_implicit

    @classmethod
    def get(cls, value: str):
        """
        Get enum type for OSM key value. Returns None if not found.
        """
        if not value:
            return None
        for member in cls:
            if member.osm_name.lower() == value.lower():
                return member
        # The Java version returns null; we could default to YES if desired.
        # return cls.YES
        # print(f"Unknown barrier type: '{value}'")
        return None

    def __str__(self):
        return self.osm_name
