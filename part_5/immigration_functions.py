from typing import List, Tuple, Union

import pandas as pd
import psycopg2.extras

from config import config
from queries import insert_into
import database


I94_SECTION_DICT = {
    'regions': 3,
    'ports': 4,
    'travel_modes': 6,
    'states': 7,
    'visa': 10
}

IMMIGRATION_COLUMNS_TO_SELECT = [
    "i94yr",
    "i94mon",
    "i94res",
    "i94port",
    "i94mode",
    "i94addr",
    "arrdate",
    "depdate",
    "i94bir",
    "i94visa",
    "gender",
    "airline",
]


def clean_immigration(df: pd.DataFrame) -> pd.DataFrame:
    """Selects a subset of columns from the immigration dataframe and cleans the selection."""

    df_cleaned = df[IMMIGRATION_COLUMNS_TO_SELECT].copy()

    state_codes = {state[0] for state in get_i94_description("states")}

    # replace i94addr state codes not found in state codes translation by '99' -> other
    df_cleaned['i94addr'] = df_cleaned['i94addr'].apply(lambda x: x if x in state_codes or x is None else '99')

    # NA imputation
    df_cleaned['i94mode'] = df_cleaned['i94mode'].fillna(9)
    df_cleaned['i94bir'] = df_cleaned['i94bir'].fillna(df_cleaned['i94bir'].median())

    # SAS date conversion
    df_cleaned['arrdate'] = pd.to_timedelta(df_cleaned['arrdate'], unit='D') + pd.Timestamp('1960-1-1')
    df_cleaned['depdate'] = pd.to_timedelta(df_cleaned['depdate'], unit='D') + pd.Timestamp('1960-1-1')

    # psycopg2 does not know how to handle pandas NaT format,
    # therefore the column is converted to string with None as null values
    df_cleaned['depdate'] = df_cleaned['depdate'].apply(lambda x: x.strftime('%Y-%m-%d') if x is not pd.NaT else None)

    return df_cleaned


def read_i94_descriptions() -> List[str]:
    """Reads the I94 description file and splits the content into sections."""

    with open(config['filenames']['immigration_description']) as f:
        i94_data = f.read()

    return i94_data.split("\n\n")


def get_code_translations(sections: List[str], section_index: int) -> List[Tuple[Union[int, str], str]]:
    """Extracts code translations from a section of the 'I94_SAS_Labels_Descriptions' file
    and returns a list of tuples, where each tuple consists of the key and the value of the mapping.
    """
    translations = []
    for line in sections[section_index].split('\n'):
        if '=' in line:
            key = line.split('=')[0].replace("'", "").strip()
            val = line.split('=')[1].replace("'", "").strip()
            try:
                key = int(key)
            except ValueError:
                pass

            translations.append((key, val, ))

    return translations


def get_i94_description(description_name: str) -> List[Tuple[Union[int, str], str]]:
    """Gets a specific i94 translation from the description file."""
    i94_sections = read_i94_descriptions()

    return get_code_translations(i94_sections, I94_SECTION_DICT[description_name])


def load_i94_data_to_db():
    """"""
    regions = get_i94_description("regions")
    database.insert_data(insert_into.i94_regions, regions)

    states = get_i94_description("states")
    state_codes = {state[0] for state in states}
    database.insert_data(insert_into.i94_states, states)

    cities = []
    for city_short, city_full in get_i94_description("ports"):
        state = None
        country = None
        if ',' in city_full:
            port_full_parts = city_full.split(',')
            city = ','.join(port_full_parts[:-1]).strip()
            suffix = port_full_parts[-1].strip()
            if suffix in state_codes:
                state = suffix
                country = 'US'
            else:
                state = None
                country = suffix
        else:
            city = city_full
        cities.append((city_short, city, state, country))

    database.insert_data(insert_into.i94_ports, cities)

    travel_modes = get_i94_description("travel_modes")
    database.insert_data(insert_into.i94_travel_modes, travel_modes)

    visa = get_i94_description("visa")
    database.insert_data(insert_into.i94_visa, visa)


def load_immigration():
    """

    """
    immigration_full = pd.read_parquet('data/immigration_full/i94_apr16_sub.parquet')
    immigration_clean = clean_immigration(immigration_full)
    data = immigration_clean.itertuples(index=False, name=None)

    with database.get_db_cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_into.immigration, data, template=None, page_size=1000)
