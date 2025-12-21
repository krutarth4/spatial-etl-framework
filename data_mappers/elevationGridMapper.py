from typing import Any, List

from main_core.data_source_abc_impl import DataSourceABCImpl
import xml.etree.ElementTree as ET


class ElevationGridMapper(DataSourceABCImpl):

    def source_filter(self, data: list[Any]) -> List[Any]:
        result = self.extract_entry_links(data)
        return result

    @staticmethod
    def extract_entry_links(xml_path):
        root = ET.fromstring(xml_path)

        # XML namespaces
        ns = {
            "atom": "http://www.w3.org/2005/Atom"
        }

        # Locate the <entry> tag
        entry = root.find("atom:entry", ns)
        if entry is None:
            return []

        # Extract all <link> tags inside <entry>
        links = [
            link.attrib["href"]
            for link in entry.findall("atom:link", ns)
            if "href" in link.attrib and link.attrib["href"].endswith(".zip")
        ]

        return links
