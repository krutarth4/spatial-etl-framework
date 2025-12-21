
from typing import Optional

from graph.Link import Link


class LinkAndDirection:
    """
    Python equivalent of:
        de.fraunhofer.fokus.asct.josmr.graphData.LinkAndDirection

    Represents a link and its direction(s) of travel.
    """

    def __init__(self, link: Link, reverse: bool = False, forward: bool = False):
        self.link: Link = link
        self.reverse: bool = reverse
        self.forward: bool = forward

    # -----------------------------------------------------
    # Alternate constructor: from DOT integer
    # -----------------------------------------------------
    @classmethod
    def from_dot(cls, link: Link, dot: int) -> "LinkAndDirection":
        """
        Construct from direction integer (Link.REVERSE / Link.FORWARD / Link.BOTH / Link.NO).
        """
        reverse = dot in (Link.REVERSE, Link.BOTH)
        forward = dot in (Link.FORWARD, Link.BOTH)
        return cls(link, reverse=reverse, forward=forward)

    # -----------------------------------------------------
    # Getter for DOT (direction of travel)
    # -----------------------------------------------------
    def get_dot(self) -> int:
        """
        Get direction of travel.
        Returns one of Link.BOTH, Link.FORWARD, Link.REVERSE, Link.NO.
        """
        if self.forward and self.reverse:
            return Link.BOTH
        elif self.forward:
            return Link.FORWARD
        elif self.reverse:
            return Link.REVERSE
        else:
            return Link.NO

    # -----------------------------------------------------
    # Alternate constructor for single direction
    # -----------------------------------------------------
    @classmethod
    def from_reverse_flag(cls, link: Link, reverse: bool) -> "LinkAndDirection":
        """
        Construct from reverse flag (forward = not reverse).
        """
        return cls(link, reverse=reverse, forward=not reverse)

    # -----------------------------------------------------
    # String representation
    # -----------------------------------------------------
    def __repr__(self):
        dot_label = {
            Link.BOTH: "BOTH",
            Link.FORWARD: "FORWARD",
            Link.REVERSE: "REVERSE",
            Link.NO: "NO",
        }.get(self.get_dot(), "?")

        return f"LinkAndDirection(link={self.link}, dot={dot_label}, forward={self.forward}, reverse={self.reverse})"
