# Project: Data Modeling with Postgres
A startup called Sparkify have lots of data on songs and user activity on their new music streaming app. The startup is interested in analysing their users and what songs they're listening to. So, it was required to build a database from the data files which are in the form of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app,
so as to be able to easily query and analyse the data.
This was done using Postgres database with tables designed to optimize queries on song play analysis. So a databse schema and ETL pipeline was created for this analysis. 


# How to run the Python scripts?

To be able to successfully execute the ETL pipleline, the .py scripts should be executed in the following sequence:
1. create_tables.py
2. etl.py

The purpose of the create_tables.py is to clear all tables from past runs, recreate the tables and establish all queries to be used in the etl.py file. 

a test notebook is available to look at the data after being loaded into the database as well as test the data types and constraints for possible changes in the future.


# The Files/data used explained:


## Data (Dataset) used
- log_data
- song_data


## Notebooks (creating and testing the pipeline):
- etl.ipynb (step by step creation of  ETL pipeline)
- test.ipynb (testing the data and data types/constraints)

## Python scripts
- sql_queries.py (contains the queries used in the create_tables.py and the etl.py files)
- create_tables.py (database restoration and tables creation script)
- etl.py (the ETL pipeline)



# Database Schema

## Fact table: Songplays
- songplay_id: PRIMARY KEY (auto-increment)
- start_time: REFERENCES time (start_time) -->FK
- user_id: REFERENCES users (user_id) -->FK
- level
- song_id: REFERENCES songs (song_id) -->FK
- artist_id: REFERENCES artists (artist_id) -->FK
- session_id
- location
- user_agent


## Dimension table s: 
# users
- user_id: PRIMARY KEY
- first_name
- last_name
- gender
- level

# songs
- song_id: PRIMARY KEY
- title
- artist_id: REFERENCES artists (artist_id) --> FK
- year
- duration

# artists
- artist_id: PRIMARY KEY
- name
- location
- latitude
- longitude

# time
- start_time: PRIMARY KEY
- hour
- day
- week
- month
- year
- weekday




