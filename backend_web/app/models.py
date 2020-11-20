from sqlalchemy.ext.automap import automap_base
from app import db

Base = automap_base()
Base.prepare(db.engine, reflect=True)

Company = Base.classes.companies
Match = Base.classes.matches