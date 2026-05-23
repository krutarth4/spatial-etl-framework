from sqlalchemy import BigInteger, CheckConstraint, Column, Integer

from database.base import Base


class WaysBaseChangesStateTable(Base):
    """Single-row table holding the current change-set generation. Bumped every
    time populate_base_graph_table produces a non-empty diff so consumers can
    detect work they haven't processed yet."""

    __tablename__ = "ways_base_changes_state"

    id = Column(Integer, primary_key=True)
    generation = Column(BigInteger, nullable=False, default=0)

    __table_args__ = (
        CheckConstraint("id = 1", name="ways_base_changes_state_singleton"),
    )
