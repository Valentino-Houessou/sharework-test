
from sqlalchemy import Column, Integer, String
from matching.models.shared import Base


class Company(Base):
    """Company entity"""
    __tablename__ = "companies"
    source_id = Column(Integer, primary_key=True)
    source_name = Column(String(100), primary_key=True)
    name = Column(String(100), nullable=False)
    website = Column(String(1000))
    email = Column(String(100))
    phone = Column(String(100))
    address = Column(String(2000))
    postal_code = Column(String(100))
    city = Column(String(100))
    country = Column(String(100))
