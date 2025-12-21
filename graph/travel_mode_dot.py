import logging
from typing import Dict, Optional
from enum import Enum

from graph.Link import Link
from graph.osm_travel_modes import OsmTravelModes



# -----------------------
# Supporting placeholders
# -----------------------



class HighwayTagEnhancer:
    """Dummy placeholder for implicit tag enhancer."""
    @staticmethod
    def determine_implicit_tags(tags: Dict[str, str]) -> Dict[str, str]:
        # Placeholder: In real use, enrich tags logically (e.g., infer bicycle=yes)
        return tags


# -----------------------
# Main Class Translation
# -----------------------

class TravelModesDot:
    """
    The information if a travel mode can traverse the link in forward or reverse direction.
    """

    def __init__(self):
        self.general_dot: Optional[int] = None
        self.travel_mode2dot: Dict[OsmTravelModes, int] = {}

    @staticmethod
    def get_from_way(way):
        """
        Get travel modes from an OSM way.
        :param way: OSM way object with id and tags attributes.
        :return: TravelModesDot instance.
        """
        # Convert tags list to a dict
        tags_original = {key: value for key, value in dict(way.tags).items()}
        tags = HighwayTagEnhancer.determine_implicit_tags(tags_original)

        modes = TravelModesDot()

        # Handle general oneway
        oneway = tags.get("oneway")
        if oneway:
            oneway_int = TravelModesDot.compute_oneway_type(oneway)
            if oneway_int is not None:
                modes.general_dot = oneway_int
        else:
            modes.general_dot = Link.BOTH

        # Define no/yes values
        no_values = {"use_sidepath", "no", "private"}
        yes_values = {"designated", "optional_sidepath", "permissive", "dismount", "yes"}

        # Iterate over tags for mode-specific access (e.g., bicycle=no)
        for key, value in tags.items():
            osm_travel_mode = OsmTravelModes.get(key)
            if osm_travel_mode:
                val_lower = (value or "").lower()
                if val_lower in no_values:
                    modes.travel_mode2dot[osm_travel_mode] = Link.NO
                elif val_lower in yes_values:
                    modes.travel_mode2dot[osm_travel_mode] = (
                        modes.general_dot if modes.general_dot is not None else Link.BOTH
                    )

                if value and value.lower() == "designated":
                    if OsmTravelModes.ACCESS not in modes.travel_mode2dot:
                        modes.travel_mode2dot[OsmTravelModes.ACCESS] = Link.NO

        # Handle oneway:<mode> tags
        for key, value in tags.items():
            if key and key.startswith("oneway:"):
                key_mode = key[7:]
                mode = OsmTravelModes.get(key_mode)
                if mode is None:
                    # logger.debug(f"No travel mode for string '{key_mode}' at way {way.id}")
                    continue

                oneway_type_int = TravelModesDot.compute_oneway_type(value)
                if oneway_type_int is not None:
                    modes.travel_mode2dot[mode] = oneway_type_int

        return modes

    @staticmethod
    def compute_oneway_type(oneway: str) -> Optional[int]:
        """
        Get numerical value representation from oneway tag.
        yes=1; no=2; -1 or reverse=-1; reversible=0
        """
        if oneway.lower() == "yes":
            return Link.FORWARD
        if oneway.lower() == "no":
            return Link.BOTH
        if oneway in ("-1", "reverse"):
            return Link.REVERSE
        if oneway.lower() == "reversible":
            return Link.NO
        if oneway.lower() == "alternating":
            return Link.BOTH

        # logger.debug(f"Unknown oneway type '{oneway}'")
        return None

    def __repr__(self):
        return f"TravelModesDot(general_dot={self.general_dot}, travel_mode2dot={self.travel_mode2dot})"
