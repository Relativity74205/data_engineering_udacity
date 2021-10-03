from typing import List, Tuple, Union, Set, Any

import pandas as pd
import numpy as np

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
    """Loads the configuration of the i94 data to database.

    In case of the ports section, the data is additionally processed to extract
    e.g. the state_code.

    In case of the states section, the data is enriched by state demographics data."""
    regions = get_i94_description("regions")
    database.insert_data(insert_into.i94_regions, regions)

    states = get_i94_description("states")
    states_enriched = enrich_i94_states(states)
    database.insert_data(insert_into.i94_states, states_enriched)

    travel_modes = get_i94_description("travel_modes")
    database.insert_data(insert_into.i94_travel_modes, travel_modes)

    visa = get_i94_description("visa")
    database.insert_data(insert_into.i94_visa, visa)

    state_codes = {state[0] for state in states}
    cities = get_i94_ports(state_codes)
    database.insert_data(insert_into.i94_ports, cities)


def enrich_i94_states(states: List[Tuple[str, str]]) -> List[Tuple[Any, ...]]:
    """Enriches the states with demographics data."""

    states_clean = []

    # corrects typos in state names and removes abbreviations
    for state_code, state in states:
        state = (state
                 .title()
                 .replace('N.', 'North')
                 .replace('S.', 'South')
                 .replace('W.', 'West')
                 .replace('Dist. Of', 'District of')
                 .replace('Wisconson', 'Wisconsin')
                 )
        states_clean.append((state_code, state))

    df_states = pd.DataFrame(states_clean, columns=["state_code", "state"])
    state_demographics = pd.read_csv(config["filenames"]["state_demographics"], skiprows=2).drop("Footnotes", axis=1).fillna(0)
    df_states = df_states.merge(state_demographics, how="left", left_on="state", right_on="Location").drop("Location",
                                                                                                           axis=1)
    df_states.columns = [col.lower().replace('/', '_').replace(' ', '_') for col in df_states.columns]
    df_states = df_states.replace({np.nan: None})

    return df_states.itertuples(index=False, name=None)


def get_i94_ports(state_codes: Set[str]) -> List[Tuple[str, str, str, str]]:
    """Extracts the raw port information from the i94 configuration file. Subsequently the
    port information is processed to extract the state_code and/or the country from the port name."""
    ports = []
    ports_raw = get_i94_description("ports")
    ports_raw = clean_ports(ports_raw)

    for port_code, port in ports_raw:
        state = None
        country = None
        if ',' in port:
            port_full_parts = port.split(',')
            port = ','.join(port_full_parts[:-1]).strip()
            suffix = port_full_parts[-1].strip()
            if suffix[:2] in state_codes:
                state = suffix[:2]
                country = 'US'
            else:
                state = None
                country = suffix

        ports.append((port_code, port, state, country))
    return ports


def clean_ports(ports_raw: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """Removes some typos from the port section of the configuration file."""
    ports_raw_cleaned = []
    for port_code, port in ports_raw:
        if port == 'MARIPOSA AZ':
            port = 'MARIPOSA, AZ'
        if port == 'WASHINGTON DC':
            port = 'WASHINGTON, DC'
        ports_raw_cleaned.append((port_code, port))

    return ports_raw_cleaned


def load_immigration() -> pd.DataFrame:
    return pd.read_sas(config["filenames"]["immigration_description"], encoding="ISO-8859-1")


def immigration_etl():
    """Loads the immigration data from the local filesystems, cleans it and saves it to the DB."""
    load_i94_data_to_db()
    immigration = load_immigration()
    immigration_clean = clean_immigration(immigration)
    database.bulk_insert_data(insert_into.immigration, immigration_clean)
