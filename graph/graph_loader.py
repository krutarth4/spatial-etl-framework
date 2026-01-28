from collections import defaultdict

import osmium
from geoalchemy2.shape import from_shape
from osmium import SimpleHandler
from shapely.geometry.linestring import LineString

from graph.osm_barrier_type import OsmBarrierType
from graph.GeoHelpers import GeoHelpers
from graph.Link import Link
from graph.geo_point import GeoPoint
from graph.max_speed_data import MaxSpeedData
from graph.osm_highway_type import OsmHighwayType
from graph.osm_smoothness import OsmSmoothness
from graph.osm_surface import OsmSurface
from graph.osm_travel_modes import OsmTravelModes
from graph.travel_mode_dot import TravelModesDot
from log_manager.logger_manager import LoggerManager
from main_core.core_config import CoreConfig



class NodeRefCounter(SimpleHandler):
    nodeSet = {}

    def __init__(self, highway_types):
        self.highway_types = highway_types
        self.highway_ways = []
        self.node_reference_counter = {}
        self.barrier_nodes = {}
        self.logger = LoggerManager(type(self).__name__).get_logger()

        self.no_values = {"no", "private"}
        self.yes_values = {"designated", "permissive", "dismount", "yes"}

    def way(self, w):
        # Equivalent to: if (getHighwayTag(way, highwayTypes) != null)
        tags = dict(w.tags)
        if NodeRefCounter.get_highway_tag(w, self.highway_types) is not None:
            for node_ref in w.nodes:
                self.node_reference_counter[node_ref.ref] = self.node_reference_counter.get(node_ref.ref, 0) + 1

    def node(self, n):
        # Equivalent to barrier node detection

        barrier = NodeRefCounter.generate_barrier_node(n)
        # self.logger.info(f"processing node {n.id}")
        if barrier is not None :
            if NodeRefCounter.is_complete(barrier):
                self.barrier_nodes[n.id] =barrier



    @staticmethod
    def get_highway_tag(way: osmium.osm.types.Way, highway_values: set):
        tags = dict(way.tags)
        # print(f"TAgs highways {(tags)}")
        for key,value in tags.items():
            if key.lower() =="highway":
                if highway_values is None or value in highway_values:
                    return value
                break
        return None

    @classmethod
    def generate_barrier_node(cls, node):
        """
        Create a BarrierNode object from an OSM node, similar to the Java version.
        Returns None if the node is not a barrier.
        """
        # node.tags in PyOsmium behaves like a dict[str, str]
        tags = dict(node.tags)
        barrier_value = tags.get("barrier")

        no_values = {"no", "private"}
        yes_values = {"designated", "permissive", "dismount", "yes"}

        allowed_modes = set()
        restricted_modes = set()

        if barrier_value is not None:
            barrier_type = OsmBarrierType.get(barrier_value)
            node_id = node.id

            # Check travel modes (like car, bicycle, foot, etc.)
            for travel_mode in OsmTravelModes:
                value = tags.get(travel_mode.key)
                if value:
                    if value in yes_values:
                        allowed_modes.add(travel_mode)
                    elif value in no_values:
                        restricted_modes.add(travel_mode)

            # Construct and return the BarrierNode
            # return BarrierNode(node_id, barrier_type, allowed_modes, restricted_modes)
            return {
                'node_id': node_id,
                'barrier_type': barrier_type,
                'modes_allowed': allowed_modes,
                'modes_restricted': restricted_modes
            }
        else:
            # Not a barrier
            return None

    @classmethod
    def is_complete(cls, barrier):
        return barrier.get("barrier_type") is not None


class NodeExc:
    def __init__(self, node: osmium.osm.types.Node):
        self.id = node.id
        self.latitude = node.lat
        self.longitude = node.lon
        self.tags = {}
        for tag in node.tags:
            key = getattr(tag, 'key', None)
            value = getattr(tag, 'value', None)
            if key is not None and key not in self.tags:
                self.tags[key] = value

# pass 2
def construct_link(w, travel_mode_dots, max_speed_data, street_name, nodes_of_link, link_index, idx_from, idx_to,
                   attributes, smoothness, surface, osm_highway_type):
    """
        Construct a Link from a section of an OSM Way.
        Equivalent to the Java constructLink() method.
        """

    # start and end nodes
    start_node = nodes_of_link[0]
    end_node = nodes_of_link[-1]

    # if osm_highway_type:
    #     if isinstance(osm_highway_type[0], dict):
    #         highway_keys = [list(h.keys())[0] for h in osm_highway_type]
    #     else:
    #         highway_keys = list(osm_highway_type)
    # else:
    #     highway_keys = None

    # create geometry (list of GeoPoints)
    geometry = [GeoPoint(node.latitude, node.longitude) for node in nodes_of_link]
    geometry_normalize = [(node.longitude, node.latitude) for node in nodes_of_link]
    linestring = from_shape(LineString(geometry_normalize), srid=4326)

    # compute length in meters
    meters = 0.0
    for i in range(len(geometry) - 1):
        p1 = geometry[i]
        p2 = geometry[i + 1]
        meters += GeoHelpers.distance_in_meters(p1.longitude, p1.latitude, p2.longitude, p2.latitude)

    # build and return Link
    return {
        "way_id":w.id,
        "way_link_index":link_index,
        "start_node_id":start_node.id,
        "end_node_id":end_node.id,
        "geometry":linestring,
        # "highway_type":osm_highway_type.osm_name if osm_highway_type is not None else osm_highway_type,
        "meters":float(meters),
        # "travel_mode_dots":travel_mode_dots.general_dot if travel_mode_dots else travel_mode_dots,
        "max_speed_forward":max_speed_data.forward if max_speed_data else None,
        "max_speed_reverse":max_speed_data.reverse if max_speed_data else None,
        "name":street_name,
        # "attributes":[a.key for a in attributes if a is not None],
        "smoothness":smoothness.key if smoothness is not None else smoothness ,
        # "surface":surface.key if surface else surface,
    }



class LinkBuilderHandler(SimpleHandler):


    def __init__(self, node_ids_of_graph, tower_nodes, highway_types):
        super().__init__()
        self.logger = LoggerManager(type(self).__name__).get_logger()

        self.node_ids_of_graph = node_ids_of_graph
        self.tower_nodes = tower_nodes
        self.highway_types = highway_types

        self.id2Nodes = {}
        self.wayId2Links = defaultdict(list)
        self.turn_restrictions = []

    def node(self, n):
        """Store graph-relevant nodes"""
        if n.id in self.node_ids_of_graph:
            self.id2Nodes[n.id] = NodeExc(n)

    def way(self, w: osmium.osm.types.Way):
        """Split OSM ways into Link objects"""
        if LinkBuilderHandler.get_highway_tag(w,self.highway_types) is None:
            return
        travel_mode_dots: TravelModesDot =  TravelModesDot.get_from_way(w)
        max_speed_data = MaxSpeedData.from_way(w)
        street_name= LinkBuilderHandler.get_street_name(w)
        attributes = LinkBuilderHandler.get_link_attributes(w)

        osm_highway_type= LinkBuilderHandler.process_highway_type(w, attributes)

        smoothness = LinkBuilderHandler.get_surface_smoothness(w)
        surface = LinkBuilderHandler.get_way_surface(w)

        way_nodes = list(w.nodes)
        # print(f"ways nodes{(way_nodes[0].ref)}")
        split_indices = [
            i for i, wn in enumerate(way_nodes)
            if i == 0 or i == len(way_nodes) - 1 or wn.ref in self.tower_nodes
        ]

        for i in range(len(split_indices) - 1):
            # print(i,  len(split_indices))
            idx_from, idx_to = split_indices[i], split_indices[i + 1]
            # print(idx_from, idx_to)
            node_of_link = [
                self.id2Nodes.get(wn.ref) for wn in way_nodes[idx_from:idx_to + 1]
                if wn.ref in self.id2Nodes
            ]

            # print(f" node segment:{node_of_link} ")
            if len(node_of_link) >= 2:
                link = construct_link(
                    w=w,
                    travel_mode_dots=travel_mode_dots,
                    max_speed_data=max_speed_data,
                    street_name=street_name,
                    nodes_of_link=node_of_link,
                    link_index=i,
                    idx_from=idx_from,
                    idx_to=idx_to,
                    attributes=attributes,
                    smoothness=smoothness,
                    surface=surface,
                    osm_highway_type=osm_highway_type,
                )

                # self.wayId2Links.setdefault(w.id,[]).append(link)
                self.wayId2Links[w.id].append(link)

    # def relation(self, r):
    #     """Extract turn restrictions"""
    #     if dict(r.tags).get('type') == 'restriction':
    #         trs = TurnRestriction.construct_from_relation(r, self.wayId2Links)
    #         if trs is not None:
    #             self.turn_restrictions.extend(trs)





    @staticmethod
    def get_highway_tag(way: osmium.osm.types.Way, highway_values: set|None):
        for key, value in dict(way.tags).items():
            if key.lower() =="highway":
                if highway_values is None or value in highway_values:
                    return value
                break
        return None


    @staticmethod
    def get_link_attributes(way):
        """
        Extract link attributes (roundabout, tunnel, bridge, etc.) from OSM way tags.

        :param way: Way object with .tags (list of Tag objects or dict)
        :return: set of Link.Attributes
        """
        results = set()

        # Convert tags to dictionary
        if isinstance(way.tags, dict):
            tags = way.tags
        else:
            tags = {key: value for key, value in dict(way.tags).items()}

        # junction = roundabout / circular
        junction_tag = tags.get("junction")
        if junction_tag:
            if junction_tag == "roundabout":
                results.add(Link.Attributes.ROUNDABOUT)
            elif junction_tag == "circular":
                results.add(Link.Attributes.CIRCULAR)

        # tunnel
        tunnel_tag = tags.get("tunnel")
        if tunnel_tag:
            if tunnel_tag == "yes":
                results.add(Link.Attributes.TUNNEL)
            elif tunnel_tag == "building_passage":
                results.add(Link.Attributes.BUILDING_PASSAGE)
            else:
                # other tunnel types also count as tunnel
                results.add(Link.Attributes.TUNNEL)

        # bridge
        bridge_tag = tags.get("bridge")
        if bridge_tag:
            results.add(Link.Attributes.BRIDGE)

        # motorroad=yes or highway=motorway
        motorroad_tag = tags.get("motorroad")
        if motorroad_tag == "yes":
            results.add(Link.Attributes.MOTORROAD)
        else:
            highway_tag = tags.get("highway")
            if highway_tag == "motorway":
                results.add(Link.Attributes.MOTORROAD)

        # lit
        lit_tag = tags.get("lit")
        if lit_tag:
            if lit_tag.lower() in ("yes", "24/7"):
                results.add(Link.Attributes.LIT)
            elif lit_tag.lower() in ("no", "disused"):
                results.add(Link.Attributes.NOT_LIT)

        # highway=construction
        highway_tag = tags.get("highway")
        if highway_tag and highway_tag.lower() == "construction":
            results.add(Link.Attributes.CONSTRUCTION)

        # sidepath: footway=sidewalk or is_sidepath=yes
        footway_tag = tags.get("footway")
        if footway_tag and footway_tag.lower() == "sidewalk":
            results.add(Link.Attributes.SIDEPATH)

        is_sidepath_tag = tags.get("is_sidepath")
        if is_sidepath_tag and is_sidepath_tag.lower() == "yes":
            results.add(Link.Attributes.SIDEPATH)

        return results

    @staticmethod
    def get_street_name(way):
        """
        Extract the street name from an OSM Way object in the following order:
          1. name
          2. ref
          3. int_ref
          4. reg_name
        Returns None if no suitable tag is found.
        """

        # Convert tags to dictionary
        if isinstance(way.tags, dict):
            tags = way.tags
        else:
            # assumes list of Tag objects with .key and .value attributes
            tags = {key: value for key, value in dict(way.tags).items()}

        # 1. name
        name_tag = tags.get("name")
        if name_tag:
            return name_tag

        # 2. ref
        ref_tag = tags.get("ref")
        if ref_tag:
            return ref_tag

        # 3. int_ref
        int_ref_tag = tags.get("int_ref")
        if int_ref_tag:
            return int_ref_tag

        # 4. reg_name
        reg_name_tag = tags.get("reg_name")
        if reg_name_tag:
            return reg_name_tag

        # None found
        return None

    @classmethod
    def process_highway_type(cls, w, attributes):
        """
            Determine the highway type for a given OSM way.
            Mirrors the Java logic:
            - If the way is under construction, use the 'construction=*' tag.
            - Otherwise, use the 'highway=*' tag.
            - Default to UNCLASSIFIED if missing during construction.
            """

        highway_tag = None
        is_construction_link = Link.Attributes.CONSTRUCTION in attributes

        # --- CASE 1: Construction link ---
        if is_construction_link:
            # Example: highway=construction, construction=residential
            construction_tag = dict(w.tags).get('construction')
            if construction_tag:
                highway_tag = construction_tag
        else:
            # --- CASE 2: Normal link ---
            highway_tag = cls.get_highway_tag(w, None)

        # --- Determine OSM highway type ---
        osm_highway_type = OsmHighwayType.get(highway_tag) if highway_tag else None

        if osm_highway_type is None:
            if is_construction_link:
                # Fallback for construction links without valid tag
                osm_highway_type = OsmHighwayType.UNCLASSIFIED
            else:
                # logger.error(f"Unable to get highway type of way {getattr(way, 'id', '?')}")
                # logger.info(f"Discarding way: {getattr(way, 'id', '?')}")
                return None

        return osm_highway_type

    @classmethod
    def get_surface_smoothness(cls, way) -> OsmSmoothness|None:
        """
            Extract the smoothness attribute from an OSM way.
            Equivalent to Java's getSurfaceSmoothness(Way way).

            :param way: osmium.osm.types.Way or compatible object with .tags
            :return: OsmSmoothness enum instance or None if not found
            """
        # Convert tags to a dictionary if needed
        tags = dict(way.tags) if hasattr(way, "tags") else {}

        smoothness_tag = tags.get("smoothness")
        if not smoothness_tag:
            return None

        # Normalize deprecated "very_good" value to "excellent"
        if smoothness_tag.lower() == "very_good":
            smoothness_tag = "excellent"

        return OsmSmoothness.get(smoothness_tag)

    @classmethod
    def get_way_surface(cls, way):
        """
            Extract the 'surface' attribute from an OSM way.

            Equivalent to Java's getWaySurface(Way way).

            :param way: osmium.osm.types.Way or similar object with .tags
            :return: OsmSurface enum instance or None if not found
            """
        # Convert tags to a dictionary (pyosmium exposes tags as mapping)
        tags = dict(way.tags) if hasattr(way, "tags") else {}

        surface_tag = tags.get("surface")
        if not surface_tag:
            return None

        # Normalize legacy/incorrect surface tags
        if surface_tag.lower() == "cobblestone:flattened":
            # old tag, should map to 'sett'
            surface_tag = "sett"
        elif surface_tag.lower() == "ash":
            # 'ash' often used for 'clay' in Germany
            surface_tag = "clay"

        return OsmSurface.get(surface_tag)

if __name__ == '__main__':
    highway_types_standard = OsmHighwayType.get_all_highway_tags(True)
    handler = NodeRefCounter(highway_types_standard)
    file_loc = "../raw/ernst_cropped.osm.pbf"
    handler.apply_file(file_loc, locations=True)


    tower_nodes = {nid for nid, count in handler.node_reference_counter.items() if count > 1}

    print(f"len tower nodes length {len(tower_nodes)}")
    print(f"barrier nodes length {len(handler.barrier_nodes)}")



    # Add barrier nodes as tower nodes
    tower_nodes.update(handler.barrier_nodes.keys())
    print(f"len tower nodes after barrier nodes included length {len(tower_nodes)}")

    node_ids_of_graph = handler.node_reference_counter.keys()
    print(f"nod id of graphs {len(node_ids_of_graph)}")

    #
    #
    handler_2 = LinkBuilderHandler(node_ids_of_graph, tower_nodes, OsmHighwayType.get_all_highway_tags(True))
    handler_2.apply_file(file_loc, locations=True)


    print("length wayId2links", len(handler_2.wayId2Links))
    print("length wayId2links", len(handler_2.id2Nodes))
    keys = list(handler_2.wayId2Links.keys())[0]
    print(f"Wayid values {len(handler_2.wayId2Links[keys])}")
    links = [link for sublist in handler_2.wayId2Links.values() for link in sublist]
    print(f"length of links {len(links)}")
    print(links[0])





    # Database bulk insert

    core_db_conf = CoreConfig().get_value("database")
    # dbConf = DbConf(core_db_conf)

    # conn = DBConnect(dbConf)
    # conn.create_all_tables()
    # db_crud = DbRepo(conn)

    # db_crud.bulk_upsert("barrier_nodes",list(handler.barrier_nodes.values()), "node_id" )
    # db_crud.bulk_upsert("links",links, "id" )
    # for link in links:
    #     db_crud.insert("links", link, None)