import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

"""
    This function processes a song file whose filepath has been provided
    as an argument. It extracts the song information in order to store it
    into the songs table. Then it extracts the artist information in order to
    store it into the artists table.

    INPUTS:
    * cur: the cursor variable
    * filepath: the file path to the song file
"""
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year',
                         'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location',
                           'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)

"""
    This function processes a log file whose filepath has been provided
    as an argument. Firstly, it extracts the timestamp information in order to
    divide it into year, month, day, hour, day of the week and week of the year.
    After storing timestamp data into the time table, it extracts user data
    and stores it into the user table. Then, based on artist name, song title
    and song duration, it searches for the song id and artist id in order to
    store those into the songplay table together with some of the data
    provenient from log data.

    INPUTS:
    * cur: the cursor variable
    * filepath: the file path to the song file
"""
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['t'] = df['ts'].apply(lambda x: datetime.fromtimestamp(x/1000.0))

    # insert time data records
    time_data = {'timestamp': df['t'],
             'year': df['t'].dt.year,
             'month': df['t'].dt.month,
             'day': df['t'].dt.day,
             'hour': df['t'].dt.hour,
             'week_day': df['t'].dt.weekday,
             'week_year': df['t'].dt.week,
            }
    time_df = pd.DataFrame.from_dict(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df[user_df.duplicated() == False]

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                            row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

"""
    This function gets all of the files matching an extension from the directory
    specified in filepath. Then it iterates over all the files found and
    processes them via the function process_song_file and process_log_file.

    INPUTS:
    * cur: the cursor variable
    * conn: the connection variable, which is linked to the postgresql database
    * filepath: the file path to the song file
    * func: the function name (process_song_file or process_log_file)
"""
def process_data(cur, conn, filepath, func):
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

"""
    This main function connects to the postgresql database and processes data
    into the 5 relational tables. Then, it closes the connection to postgresql.
"""
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
