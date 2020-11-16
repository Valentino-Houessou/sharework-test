import pandas as pd
from matching.models.Company import Company
from matching.utils import company_col_names


def load_companies(file_path, source_name, session):
    try:
        dataset = pd.read_csv(file_path, names=company_col_names, header=None)
        dataset = dataset.values.tolist()

        for row in dataset:
            exists = session.query(Company).filter_by(
                source_id=row[0], source_name=source_name).first()
            if not exists:
                record = Company(source_id=row[0], source_name=source_name,
                                 name=row[1], website=row[2], email=row[3],
                                 phone=row[4], address=row[5], postal_code=row[6],
                                 city=row[7], country=row[8])
                session.add(record)

        session.commit()
    except Exception as error:
        print("Oops! An exception has occured:", error)
        print("Exception TYPE:", type(error))
