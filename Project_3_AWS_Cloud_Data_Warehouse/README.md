# Project: Data Warehouse for Sparkify
## Purpose
The purpose of this database is to provide a data warehouse for the music streaming startup, Sparkify, to better understand their user's listening habits and gain insights into their app's performance. The data warehouse will be optimized for queries on song play analysis.

# Database Schema Design
In order to meet the analytical goals of Sparkify, a star schema has been chosen as the best design for this data warehouse. The star schema consists of one fact table, songplays, and four dimension tables, users, songs, artists, and time.

The fact table,songplays, is used to store information about the songs that have been played by the users, including the start_time, user_id, level, song_id, artist_id, session_id, location, and user_agent. This table is connected to the dimension tables through foreign keys.

The dimension tables store information about the users, songs, artists, and time of the song plays. The users table includes information about the users such as user_id, first_name, last_name, gender, and level. The songs table includes information about the songs, such as song_id, title, artist_id, year, and duration. The artists table includes information about the artists, such as artist_id, name, location, latitude, and longitude. The time table includes information about the time of the song plays, such as start_time, hour, day, week, month, year, and weekday.

# ETL Pipeline
The ETL pipeline for this data warehouse includes several steps:

1. Extracting data from S3: The data is extracted from the S3 bucket containing the song and event datasets.
2. Staging the data: The extracted data is then loaded into staging tables on Redshift.
3. Transforming the data: The data from the staging tables is then transformed and inserted into the fact and dimension tables.
In the first step, the data is extracted from S3 using the S3 links provided: s3://udacity-dend/song_data for song data and s3://udacity-dend/log_data for log data. The log_json_path.json file is also used to get the schema of the log data.

In the second step, the extracted data is then loaded into staging tables on Redshift using the SQL COPY command. The staging tables are defined in the create_table.py file and dropped in the beginning of the etl.py file if they already exist.

In the third step, the data from the staging tables is transformed and inserted into the fact and dimension tables using the SQL INSERT and SELECT commands. The SQL statements for these commands are defined in the sql_queries.py file and imported into the etl.py file. The final tables are optimized for efficient querying and analysis.

# Example Queries
Here are some example queries that can be run on the data warehouse to gain insights into the user's listening habits:
Top 10 most popular songs:

```
SELECT songs.title, COUNT(songplays.songplay_id) as plays
FROM songplays
JOIN songs ON songplays.song_id = songs.song_id
GROUP BY songs.title
ORDER BY plays DESC
LIMIT 10;


```

Number of unique users per month:


```
SELECT DATE_TRUNC('month',time.start_time) as month, COUNT(DISTINCT songplays.user_id


```