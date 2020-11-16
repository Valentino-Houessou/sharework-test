
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

from matching.matches_processing import process
from matching.models.shared import Base
from matching.data_loader import load_companies

basedir = os.path.abspath(os.path.dirname(__file__))

# Database
database_url = "sqlite:///" + \
    os.path.join(basedir, "matching_db.sqlite")

engine = create_engine(database_url, echo=False)

Base.metadata.create_all(engine)

DBSession = sessionmaker(bind=engine)
session = DBSession()

base_A_path = "data/dataset_A.csv"
base_B_path = "data/dataset_B.csv"
name_A = "base_A"
name_B = "base_B"

# load base_A data
load_companies(base_A_path, name_A, session)
# load base_B data
load_companies(base_B_path, name_B, session)

# process the matching
process(base_A_path, base_B_path, name_A, name_B, engine)
