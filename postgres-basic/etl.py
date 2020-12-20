import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Inserts relevant information from a json file into two dimension tables,
    one for songs' information, other for artists' information.

    Parameters
    ----------
    cur: psycopg2 cursor
        Must be already attached to a database

    filepath: String
        path to a single json file

    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates().values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = \
    df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Inserts relevant information from a json file into a dimension and a fact table.
    The fact table contains information about app events, in special songs played.
    The dimension table contains information about time.

    Parameters
    ----------
    cur: psycopg2 cursor
        Must be already attached to a database

    filepath: String
        path to a single json file

    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    # convert timestamp column to datetime
    t = pd.to_datetime(df.query('page == "NextSong"').ts, unit='ms')

    # insert time data records
    time_df = pd.DataFrame({
        'timestamp': t,
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week of year': t.dt.weekofyear,
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday
    })

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

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
        songplay_data = (
            pd.to_datetime(row.ts),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Inserts relevant information from a json file into a dimension and a fact table.
    The fact table contains information about app events, in special songs played.
    The dimension table contains information about time.

    Parameters
    ----------
    cur: psycopg2 cursor
        Must be already attached to a database

    conn: postgres connection
        Necessary to commit cursor execution after table insertions

    filepath: String
        path to a directory containing data to be processed using func

    func: callable
        Function that will process data files on the given directory
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()