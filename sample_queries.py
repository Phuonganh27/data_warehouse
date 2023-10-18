import configparser
import psycopg2

most_played_song = """
SELECT fs.f_song_id, ds.d_title, COUNT(*) AS played_time
FROM songplay_table fs
JOIN song_table ds
ON fs.f_song_id = ds.d_song_id
GROUP BY f_song_id, ds.d_title
ORDER BY played_time DESC
LIMIT 5;
"""

most_played_hour = """
SELECT dt.d_hour, COUNT(*) AS played_time
FROM songplay_table fs
JOIN time_table dt
ON fs.f_start_time = dt.d_start_time
GROUP BY dt.d_hour
ORDER BY played_time DESC
LIMIT 5;
"""

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    cur.execute(most_played_song)
    print("What is the most played song?")
    for row in cur:
        print(row)
    conn.commit()

    cur.execute(most_played_hour)
    print("When is the highest usage time of day by hour for songs?")
    for row in cur:
        print(row)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()