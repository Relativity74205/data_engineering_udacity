from typing import Iterator, Tuple, Any

import pandas as pd
import numpy as np

import database
from queries import insert_into
from config import config


def clean_demographics(df_demographics: pd.DataFrame) -> pd.DataFrame:
    """Cleans and preprocesses the raw demographics data.

    Following columns are dropped:

    - State (redundant)
    - Number of Veterans (not relevant for this use case)
    - Median Age (not relevant for this use case)
    - Average Household Size (not relevant for this use case)
    - Male Population (not relevant for this use case)
    - Female Population (not relevant for this use case)
    """
    demographics_basis = df_demographics[["City", "State Code", "Total Population", "Foreign-born"]].copy().drop_duplicates()

    # Pivots the race columns. The long table is transformed into a wide table for the population numbers per race.
    demographics_races = pd.pivot_table(df_demographics, values='Count', index=['City', 'State Code'], columns=['Race'],
                                        fill_value=0).reset_index()
    demographics_races.columns.name = None

    demographics_complete = demographics_basis.merge(demographics_races, on=['City', 'State Code'])
    demographics_complete.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in
                                     demographics_complete.columns]

    return demographics_complete


def prepare_demographics_for_load(df: pd.DataFrame) -> Iterator[Tuple[Any, ...]]:
    """Prepares demographics data for database load.

    Replaces all numpy nans with None (all columns are converted to object type), as psycopg2 cannot handle
    the numpy nans. Finally, converts the pd.DataFrame to List of Tuples."""
    df = df.replace({np.nan: None})
    return df.itertuples(index=False, name=None)


def load_demographics() -> pd.DataFrame:
    return pd.read_csv(config["filenames"]["demographics"], sep=";")


def demographics_etl():
    demographics_raw = load_demographics()
    demographics_clean = clean_demographics(demographics_raw)
    demographics_final = prepare_demographics_for_load(demographics_clean)
    database.insert_data(insert_into.demographics, demographics_final)
