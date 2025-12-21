import logging
from typing import List, Dict, Set, Optional

from graph.Link import Link
from graph.helperUtil import get_tags_key_starts_with, get_tag
from graph.link_and_direction import LinkAndDirection
from graph.osm_travel_modes import OsmTravelModes

logger = logging.getLogger(__name__)


class TurnRestriction:
    """
    Python equivalent of de.fraunhofer.fokus.asct.josmr.graphData.TurnRestriction
    """

    def __init__(self, restriction_type, relation_id, member_from, via, member_to,
                 modes: Set[OsmTravelModes], modes_except: Set[OsmTravelModes]):
        self.type = restriction_type
        self.relation_id = relation_id
        self.from_member = member_from
        self.via = via
        self.to_member = member_to
        self.modes = modes
        self.modes_except = modes_except
        #TODO: linkandDirection class here with from and to link check again in java
        self.from_link: Optional[Link] = None
        self.to_link: Optional[LinkAndDirection] = None

    # --------------------------------------------------------
    #  Construct TurnRestriction(s) from OSM Relation
    # --------------------------------------------------------
    @staticmethod
    def construct_from_relation(relation, way_id_to_links: Dict[int, List[Link]]) -> Optional[List["TurnRestriction"]]:
        results = []

        # 1️⃣ Parse "except" tag
        except_tag = get_tag(relation.tags, "except")
        modes_except = None
        if except_tag:
            excepts_str = except_tag
            split = excepts_str.split(";")
            modes_except = {OsmTravelModes.get(s) for s in split if OsmTravelModes.get(s)}

        # 2️⃣ Restriction type and applicable modes
        restriction_key = "restriction"
        restriction_tag = get_tag(relation.tags, restriction_key)
        modes = set()
        restriction_type = None

        if restriction_tag:
            restriction_type = RestrictionType.get(restriction_tag)

        # handle specific restriction: restriction:hgv, restriction:motorcar etc.
        restriction_types = get_tags_key_starts_with(restriction_key, relation.tags)
        for k, v in restriction_types.items():
            mode_str = TurnRestriction.extract_mode(k)
            m = OsmTravelModes.get(mode_str) if mode_str else None
            if m:
                modes.add(m)
            type_act = RestrictionType.get(v)
            if restriction_type and type_act and type_act != restriction_type:
                logger.debug(f"type differs: {type_act} & {restriction_type}")
            restriction_type = type_act

        # 3️⃣ Extract relation members by role
        members = relation.members
        from_members = TurnRestriction.get_members(members, "from")
        via_members = TurnRestriction.get_members(members, "via")
        to_members = TurnRestriction.get_members(members, "to")

        # --- Validation ---
        if not from_members or not via_members or not to_members:
            logger.debug(f"incomplete roles for restriction: {relation.id}")
            return None

        # multiple FROM or TO checks
        if len(from_members) > 1 and restriction_type != RestrictionType.NO_ENTRY:
            return None
        if len(to_members) > 1 and restriction_type != RestrictionType.NO_EXIT:
            return None
        if len(from_members) > 1 and len(to_members) > 1:
            return None

        # 4️⃣ Expand multiple FROM or TO into multiple restrictions
        if len(from_members) > 1:
            for member_from in from_members:
                results.append(TurnRestriction(restriction_type, relation.id,
                                               member_from, via_members, to_members[0],
                                               modes, modes_except))
        elif len(to_members) > 1:
            for member_to in to_members:
                results.append(TurnRestriction(restriction_type, relation.id,
                                               from_members[0], via_members, member_to,
                                               modes, modes_except))
        else:
            results.append(TurnRestriction(restriction_type, relation.id,
                                           from_members[0], via_members, to_members[0],
                                           modes, modes_except))

        # 5️⃣ Apply link direction
        for tr in results:
            tr.from_link = TurnRestriction.apply_link_role_from(tr, way_id_to_links)
            tr.to_link = TurnRestriction.apply_link_role_to(tr, way_id_to_links)

        return results

    # --------------------------------------------------------
    # Helpers
    # --------------------------------------------------------

    @staticmethod
    def extract_mode(key: str) -> Optional[str]:
        """Extract mode from 'restriction:hgv' → 'hgv'"""
        split = key.split(":")
        return split[1] if len(split) >= 2 else None

    @staticmethod
    def get_members(members, role: str) -> List["Member"]:
        """
        Get relation members with a given role ('from', 'via', 'to')
        """
        result = []
        for m in members:
            if hasattr(m, "member_role") and m.member_role.lower() == role.lower():
                member_type = TurnRestriction.translate_entity_to_member_type(m.member_type)
                result.append(Member(member_type, m.member_id))
        return result

    @staticmethod
    def translate_entity_to_member_type(entity_type):
        """Map Osmosis EntityType → MemberType"""
        if str(entity_type).lower() == "node":
            return MemberType.NODE
        elif str(entity_type).lower() == "way":
            return MemberType.WAY
        else:
            logger.error(f"Unknown entity type: {entity_type}")
            return None

    # --------------------------------------------------------
    # Link direction determination
    # --------------------------------------------------------

    @staticmethod
    def apply_link_role_from(tr: "TurnRestriction", way_id_to_links: Dict[int, List[Link]]) -> Optional["LinkAndDirection"]:
        via_first = tr.via[0]
        node_ids_via = set()

        if via_first.type == MemberType.NODE:
            node_ids_via.add(via_first.id)
        else:
            links = way_id_to_links.get(via_first.id)
            if not links:
                return None
            node_ids_via.add(links[0].start_node_id)
            node_ids_via.add(links[-1].end_node_id)

        links_from = way_id_to_links.get(tr.from_member.id)
        if not links_from:
            logger.debug(f"no links for 'from' {tr.from_member}")
            return None

        start_id = links_from[0].start_node_id
        end_id = links_from[-1].end_node_id
        forward = end_id in node_ids_via
        reverse = start_id in node_ids_via

        link = links_from[-1] if forward else links_from[0]
        return LinkAndDirection(link, reverse, forward)

    @staticmethod
    def apply_link_role_to(tr: "TurnRestriction", way_id_to_links: Dict[int, List[Link]]) -> Optional["LinkAndDirection"]:
        via_last = tr.via[-1]
        node_ids_via = set()

        if via_last.type == MemberType.NODE:
            node_ids_via.add(via_last.id)
        else:
            links = way_id_to_links.get(via_last.id)
            if not links:
                return None
            node_ids_via.add(links[0].start_node_id)
            node_ids_via.add(links[-1].end_node_id)

        links_to = way_id_to_links.get(tr.to_member.id)
        if not links_to:
            logger.debug(f"no links for 'to' {tr.to_member}")
            return None

        start_id = links_to[0].start_node_id
        end_id = links_to[-1].end_node_id
        forward = start_id in node_ids_via
        reverse = end_id in node_ids_via

        link = links_to[0] if forward else links_to[-1]
        return LinkAndDirection(link, reverse, forward)


# ----------------------------------------------------------------
# Nested helper classes / enums
# ----------------------------------------------------------------

class MemberType:
    NODE = "NODE"
    WAY = "WAY"


class RestrictionType:
    NO_RIGHT_TURN = ("no_right_turn", False)
    NO_LEFT_TURN = ("no_left_turn", False)
    NO_U_TURN = ("no_u_turn", False)
    NO_STRAIGHT_ON = ("no_straight_on", False)
    NO_ENTRY = ("no_entry", False)
    NO_EXIT = ("no_exit", False)
    ONLY_RIGHT_TURN = ("only_right_turn", True)
    ONLY_LEFT_TURN = ("only_left_turn", True)
    ONLY_U_TURN = ("only_u_turn", True)
    ONLY_STRAIGHT_ON = ("only_straight_on", True)

    @classmethod
    def get(cls, value: str):
        value = value.lower()
        for name, (key, only) in cls.__dict__.items():
            if isinstance(key, tuple) and key[0] == value:
                obj = type("RestrictionTypeEnum", (), {"key": key[0], "only": only})
                return obj()
        logger.error(f"Unknown restriction type: '{value}'")
        return None


class Member:
    def __init__(self, member_type: MemberType, member_id: int):
        self.type = member_type
        self.id = member_id

    def __repr__(self):
        return f"Member(type={self.type}, id={self.id})"
