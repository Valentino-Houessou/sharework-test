
from sqlalchemy import Column, Integer, Float, String, ForeignKeyConstraint
from matching.models.shared import Base


class Match(Base):
    """Match entity"""
    __tablename__ = "matches"
    id_1 = Column(Integer, nullable=False)
    id_2 = Column(Integer, nullable=False)
    source_1 = Column(String(50), nullable=False)
    source_2 = Column(String(50), nullable=False)
    name_score = Column(Float, nullable=False)
    website_score = Column(Float, nullable=False)
    phone_score = Column(Float, nullable=False)
    city_score = Column(Float, nullable=False)
    postal_code_score = Column(Float, nullable=False)
    address_score = Column(Float, nullable=False)
    global_score = Column(Float, nullable=False)
    base_1_lookup = Column(String(5000), nullable=False)
    base_2_lookup = Column(String(5000), nullable=False)
    ForeignKeyConstraint(["id_1", "source_1"], [
                         "companies.source_id", "companies.source_name"])
    ForeignKeyConstraint(["id_2", "source_2"], [
                         "companies.source_id", "companies.source_name"])
