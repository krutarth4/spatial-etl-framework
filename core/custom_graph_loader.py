from pathlib import Path

from data_config_dtos.data_source_config_dto import GraphConfDTO
from graph.graph_loader import NodeRefCounter, LinkBuilderHandler
from graph.osm_highway_type import OsmHighwayType
from handlers.file_handler import FileHandler
from log_manager.logger_manager import LoggerManager
from utils.execution_time import measure_time


class CustomGraphLoader:

    def __init__(self,graph_conf:GraphConfDTO):
        self.conf = graph_conf
        self.handler = None
        self.logger = LoggerManager(type(self).__name__).get_logger()

    @measure_time(label= "OSM initialization")
    def initialize(self):
        osm_pbf_file_location = self.get_osm_file_path()
        highway_types_standard = OsmHighwayType.get_all_highway_tags(True)
        self.handler = NodeRefCounter(highway_types_standard)
        self.handler.apply_file(osm_pbf_file_location,locations=True, idx="flex_mem")
        tower_nodes = {nid for nid, count in self.handler.node_reference_counter.items() if count > 1}

        print(f"len tower nodes length {len(tower_nodes)}")
        print(f"barrier nodes length {len(self.handler.barrier_nodes)}")

        # Add barrier nodes as tower nodes
        tower_nodes.update(self.handler.barrier_nodes.keys())
        print(f"len tower nodes after barrier nodes included length {len(tower_nodes)}")

        node_ids_of_graph = self.handler.node_reference_counter.keys()
        print(f"nod id of graphs {len(node_ids_of_graph)}")
        self.logger.info("Starting .....link Building for base graph")
        handler_2 = LinkBuilderHandler(node_ids_of_graph, tower_nodes, OsmHighwayType.get_all_highway_tags(True))
        handler_2.apply_file(osm_pbf_file_location, locations=True)

        print("length wayId2links", len(handler_2.wayId2Links))
        print("length wayId2links", len(handler_2.id2Nodes))
        keys = list(handler_2.wayId2Links.keys())[0]
        print(f"Wayid values {len(handler_2.wayId2Links[keys])}")
        links = [link for sublist in handler_2.wayId2Links.values() for link in sublist]
        print(f"length of links {len(links)}")
        print(links[0])
        return links

    def get_osm_file_path(self) -> str | None:
        osm_pbf_file_location =  self.conf.osm_file_path
        file_handler = FileHandler(osm_pbf_file_location)
        name_as_array = osm_pbf_file_location.split("/")[-1].split(".")
        file_name = ".".join(name_as_array[:-1])
        extension = name_as_array[-1]
        osm_file_path = file_handler.get_latest_data_file(file_name, extension)
        if osm_file_path is None:
            self.logger.error("osm pbf file path not found. Please check the configuration file")
            return None
        else:
            return osm_file_path


    def load(self):
        pass


