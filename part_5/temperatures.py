import pandas as pd

import database
from queries import insert_into
from config import config


def clean_temperatures(temperatures: pd.DataFrame) -> pd.DataFrame:
    """Cleans and preprocesses the raw temperatures data."""
    temperatures.columns = [col.lower() for col in temperatures.columns]
    temperatures_clean = temperatures[['dt', 'averagetemperature', 'city', 'country', 'latitude', 'longitude']].copy()

    temperatures_clean['dt'] = pd.to_datetime(temperatures_clean['dt'])
    temperatures_clean['calendar_month'] = temperatures_clean['dt'].dt.month

    return temperatures_clean


def aggregate_temperatures(temperatures: pd.DataFrame) -> pd.DataFrame:
    """Aggregates the cleaned temperatures data.

    The last 3 values per city and calendar_month are selected and the mean temperature value for
    city + calendar_month is calculated."""
    key_columns = ['city', 'country', 'latitude', 'longitude']
    temperatures_agg = (
        temperatures
            .sort_values(key_columns + ['dt'])
            .groupby(key_columns + ['calendar_month'])
            .tail(3)
    )
    temperatures_agg = (
        temperatures_agg
            .groupby(key_columns + ['calendar_month'])
            .averagetemperature
            .agg(mean_temperature='mean').reset_index()
    )

    return temperatures_agg


def load_temperatures() -> pd.DataFrame:
    return pd.read_csv(config["filenames"]["temperatures"])


def temperatures_etl():
    temperatures = load_temperatures()
    temperatures_clean = clean_temperatures(temperatures)
    temperatures_agg = aggregate_temperatures(temperatures_clean)
    database.bulk_insert_data(insert_into.temperatures, temperatures_agg)
