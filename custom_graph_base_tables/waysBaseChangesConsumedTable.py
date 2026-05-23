from sqlalchemy import BigInteger, Column, String

from database.base import Base


class WaysBaseChangesConsumedTable(Base):
    """Tracks, per datasource, the latest ways_base_changes generation that has
    been mapped into the datasource's mapping table. Used by incremental
    mapping to skip work when the consumer is already caught up."""

    __tablename__ = "ways_base_changes_consumed"

    datasource_name = Column(String(255), primary_key=True)
    consumed_generation = Column(BigInteger, nullable=False)
