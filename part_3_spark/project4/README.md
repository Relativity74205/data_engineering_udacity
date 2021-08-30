# Sparkify analytics database

## Goals

The goal of the database is to have all data about listened songs easily accessible for 
analysis. This is crucial for the startup in order to understand the customer behaviour 
and preferences. 

For example, for the startup it is important to know which songs/artists are played a lot, how many paid 
users exists, how the ratio between free and paid users develops, etc. etc.

## Usage of scripts

Run the `etl.py` script in the home directory of the EMR master node:
```shell
spark-submit etl.py
```

Prerequisite for both scripts is the config file `dl.cfg`, which must be present in the same directory as
the python file. The config file must have the below form.

```
[AWS]
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
```

## Database schema design and etl process

#### The database is set up in a star schema:
- The fact table `songplays` has an entry for each played song. In addition to 
  basic information like the `timestamp` each entry has the `id` of the corresponding 
  user, song and artist which links to the corresponding dimension table.
- The central `songplays` table links to the following dimension tables:
    - `users` table has information about the users
    - `songs` table has information about the songs
    - `artists` table has information about the artists
    - `time` table has additional information about the time when the songs were played.
  
![DB Schema](schema/results/diagrams/summary/relationships.implied.large.png)

This database design is very useful for analytics as it simplifies analytics queries a lot
and allows fast aggregations. In the case of Sparkify the relevant entity is the 
songplay, which is stored in the songplays table. Aggregations (e.g. how many songs were played
per day) can be performed very easily. If additional information are needed e.g. for grouping of filtering 
of the played songs, they are readily available in the dimension tables.


#### The ETL process consists of the following steps:
- Copying of JSON data from files located at AWS S3 to the spark cluster
- Processing of song_data
  - Extracting of song information from the songs raw data and saving the `songs` dimension table
  - Extracting of artist information from the songs raw data and saving the `artists` dimension table
- Processing of log_data (played songs)
  - Extracting of the timestamp column from the events raw data, calculating metadata for each timestamp and save to the `time` dimension table
  - Extracting of users information from the events raw data and saving the `users` dimension table
  - Extracting of songplay information from the events raw data and joining with the song raw data combining with artist_id and song_id 
  to get the `song_id` and `artist_id`
