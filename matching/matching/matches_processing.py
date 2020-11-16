import pandas as pd
import recordlinkage

from matching.utils import company_col_names


def create_lookup(row):
    lookup = ""
    if pd.notna(row["name"]):
        lookup += row["name"]
    if pd.notna(row["website"]):
        lookup += " / " + row["website"]
    if pd.notna(row["address"]):
        lookup += " / " + row["address"]
    if pd.notna(row["postal_code"]):
        lookup += " / " + row["postal_code"]
    if pd.notna(row["city"]):
        lookup += " / " + row["city"]
    if pd.notna(row["phone"]):
        lookup += " / " + row["phone"]

    return lookup.strip()


def process(file_1_path, file_2_path, name_1, name_2, engine):
    try:
        # load data into panda dataframe
        dataset_1 = pd.read_csv(
            file_1_path, names=company_col_names, header=None, index_col="id")
        dataset_2 = pd.read_csv(
            file_2_path, names=company_col_names, header=None, index_col="id")

        # lower case the string value in the dataframe
        dataframe_1 = dataset_1.applymap(
            lambda s: s.lower() if type(s) == str else s)
        dataframe_2 = dataset_2.applymap(
            lambda s: s.lower() if type(s) == str else s)

        # adding source name
        dataframe_1["source_1"] = name_1
        dataframe_2["source_2"] = name_2

        # block on name as the name field doesn't contains any empty fiels
        indexer = recordlinkage.Index()
        indexer.sortedneighbourhood(on=["name"])
        candidates = indexer.index(dataframe_1, dataframe_2)

        compare = recordlinkage.Compare()
        compare.string("name",
                       "name",
                       method="jarowinkler",
                       threshold=.90,
                       label="name_score")

        compare.string("website",
                       "website",
                       method="jarowinkler",
                       threshold=.90,
                       label="website_score")

        compare.string("phone",
                       "phone",
                       method="jarowinkler",
                       threshold=.95,
                       label="phone_score")

        compare.string("city",
                       "city",
                       method="jarowinkler",
                       threshold=.90,
                       label="city_score")

        compare.string("postal_code",
                       "postal_code",
                       method="jarowinkler",
                       threshold=.90,
                       label="postal_code_score")

        compare.string("address",
                       "address",
                       method="jarowinkler",
                       threshold=.85,
                       label="address_score")

        features = compare.compute(candidates, dataframe_1, dataframe_2)

        # create a score field to group all the compare sump
        # the row with score 6 will be the perfect match
        # the row with score 1 will be the worse match
        potential_matches = features[features.sum(axis=1) > 1].reset_index()
        potential_matches['global_score'] = potential_matches.loc[:,
                                                                  'name_score':'address_score'].sum(axis=1)

        # create lookup fields
        dataframe_1["base_1_lookup"] = dataframe_1[["name", "website",
                                                    "address", "postal_code",
                                                    "city", "phone"]].apply(lambda row: create_lookup(row), axis=1)

        dataframe_2["base_2_lookup"] = dataframe_2[["name", "website",
                                                    "address", "postal_code",
                                                    "city", "phone"]].apply(lambda row: create_lookup(row), axis=1)

        frame_1 = dataframe_1[["base_1_lookup", "source_1"]].reset_index()
        frame_2 = dataframe_2[["base_2_lookup", "source_2"]].reset_index()

        frame_1_merge = potential_matches.merge(
            frame_1, how="left", left_on="id_1", right_on="id")
        final_merge = frame_1_merge.merge(
            frame_2, how="left", left_on="id_2", right_on="id")

        # delete the unuses column
        del final_merge["id_x"]
        del final_merge["id_y"]

        # filter an keep only when we got a match on the name or the website or the phone
        with engine.connect() as conn:
            try:
                final_merge[(final_merge.name_score == 1.0) |
                            (final_merge.website_score == 1.0) |
                            (final_merge.phone_score) == 1.0].to_sql(
                    "matches", conn, if_exists="append")
            except Exception:
                print("an error occured")
            finally:
                conn.close()

    except OSError as err:
        print(err)
