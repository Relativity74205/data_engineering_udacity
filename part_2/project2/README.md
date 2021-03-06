# Sparkify analytics database

## Goals

The goal of the database is to have all data about listened songs easily accessible for 
analysis. This is crucial for the startup in order to understand the customer behaviour 
and preferences. 

For example, for the startup it is important to know which songs/artists are played a lot, how many paid 
users exists, how the ratio between free and paid users develops, etc. etc.

## Usage of scripts

To run the python scripts the `psycopg2` package has to be installed in the used python environment.

Run the `create_tables.py` script, to create all tables:
```shell
python create_tables.py
```
Run the `etl.py` script, to load the data from S3 and to run the etl process in Redshift:
```shell
python etl.py
```

Prerequisite for both scripts is the config file `dwh.cfg`, which must be present in the same directory as
the python files. The config file must have the below form, however, the values in the `cluster` and `iam_role`
section have to be adjusted.

The `iam_role` is assigned to the redshift cluster for copying of the data from S3 to redshift. It must have
the `arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess` policy.
```
[CLUSTER]
HOST='redshift-cluster-1.cmhhchzmlyns.us-east-1.redshift.amazonaws.com'
DB_NAME='dev'
DB_USER='awsuser'
DB_PASSWORD='password'
DB_PORT=5439

[IAM_ROLE]
ARN=arn:aws:iam::598463720578:role/myRedshiftRole

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
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

In addition, the database consists of two staging tables where the raw data is loaded into.

See the DDL queries and the corresponding comments in [sql_queries.py](sql_queries.py) for details about 
the tables.


#### The ETL process consists of the following steps:
- Copying of JSON data from files located at AWS S3 to staging tables (`staging_songs` and `staging_events`).
- Processing of song_data
  - Extracting of song information from the `staging_songs` table and saving the `songs` dimension table
  - Extracting of artist information from the `staging_songs` table and saving the `artists` dimension table
- Processing of log_data (played songs)
  - Extracting of the timestamp column from the `staging_events` table, calculating metadata for each timestamp and save to the `time` dimension table
  - Extracting of users information from the `staging_events` table and saving the `users` dimension table
  - Extracting of songplay information from the `staging_events` table and joining with the `staging_songs` table combining with artist_id and song_id 
  to get the `song_id` and `artist_id`

See the insert queries and the corresponding comments in [sql_queries.py](sql_queries.py) for details about 
the tables.
 

#### Example queries

- Number of songs played each day:
```sql
SELECT DATE(start_time) as date, COUNT(*) as cnt_songplays_per_day
FROM songplays
GROUP BY DATE(start_time)
ORDER BY DATE(start_time)
```
- Ratio between free and paid songplays:
```sql
SELECT level, COUNT(*) * 1.0 / (SELECT COUNT(*) FROM songplays) AS frequency
FROM songplays
GROUP BY level
```
- Distribution of songplays between the weekdays:
```sql
SELECT time.weekday, COUNT(*) as cnt_songplays_per_day
FROM songplays
JOIN time USING (start_time)
GROUP BY time.weekday
ORDER BY time.weekday
```
