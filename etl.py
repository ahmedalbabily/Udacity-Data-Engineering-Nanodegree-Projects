import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

import numpy
from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


def process_song_file(cur, filepath):
    
     """
     -Gets the data from the song file and loads it into the songs and artists table
    """
    # open song file
    df = pd.read_json(filepath, lines=True).replace({pd.np.nan: None})




    #I need to insert artist data first for fk constraint not to be violated
    
        
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0].values.tolist()

    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].iloc[0].values.tolist()

    cur.execute(song_table_insert, song_data)



def process_log_file(cur, filepath):
    
        """
     -Gets the data from the log file and loads it into the time, users and songplays tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True).replace({pd.np.nan: None})
 

    # filter by NextSong action
    df = df.query("page=='NextSong'") 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    
    # insert time data records
    time_data = (list(df['ts']),list(t.dt.hour),list(t.dt.day),list(t.dt.weekofyear),list(t.dt.month),list(t.dt.year),list(t.dt.weekday))

    column_labels = ("start_time","hour", "day" , "week", "month","year" , "weekday")

    
    
    time_df= pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))
 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
        """
     -executes the two functions process_songs_file and process_log_file on all the files in their directories
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()