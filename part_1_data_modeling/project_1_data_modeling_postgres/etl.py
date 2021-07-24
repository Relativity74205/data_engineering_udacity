import os
import glob
from typing import Callable

import psycopg2
import pandas as pd

# original import: from sql_queries import *
# however "import *" imports shouldn't be used in any case!
from sql_queries import (
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    user_table_insert,
    songplay_table_insert,
    song_select
)


def process_song_file(cur, filepath: str) -> None:
    """Processes a single song file.

    Loads the the data from one song file into a pandas dataframe. Extracts of song and artist information and saves
    it to the `songs` dimension table.
    Args:
        cur: DB cursor
        filepath: path to one song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = (df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath: str):
    """Processes a single log file.

    1. Loads the the data from one log file (consisting of multiple entries) into a pandas dataframe.
    2. Extracts the timestamp column, calculates metadata for each timestamp and saves the timestamp
    data  to the `time` dimension table.
    3. Extracts of users information from the log_data and saves it the `users` dimension table.
    4. For each song:
    - Queries database for artist_id and song_id of the played song.
    - Extracts the songplay information and combines it with the artist_id and song_id (`None` if nothing was found)
    - Saves songplay information to database.

    Args:
        cur: DB cursor
        filepath: path to one song file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (
        t.values,
        t.dt.hour.values,
        t.dt.day.values,
        t.dt.isocalendar().week.values,
        t.dt.month.values,
        t.dt.year.values,
        t.dt.weekday.values,
    )
    column_labels = (
        "start_time", "hour", "day", "week", "month", "year", "weekday"
    )
    time_df = pd.DataFrame({col_name: col_data for col_name, col_data in zip(column_labels, time_data)})

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath: str, func: Callable) -> None:
    """Processes all data files.

    Generates a list of all json-files by walking the base filepath recursively and
    searching for json-files. Then iterates over the file list and passing each file
    to the provided python callable. Commits after the processing of each file.

    Args:
        cur: DB Cursor
        conn: DB Connection
        filepath: basepath to the json files
        func: callable which is used to process one raw json data file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Main etl function.

    Creates DB connection and process first the songfile data and then log file data."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
