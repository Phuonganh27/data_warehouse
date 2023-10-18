import configparser
import psycopg2

def check_status(cur, conn):
    table_list = ['staging_events_table', 'staging_songs_table', 'songplay_table', 'user_table', 'song_table', 'artist_table', 'time_table']
    for table in table_list:
        query = """SELECT COUNT(*) FROM {}""".format(table)
        print(query) #debug
        cur.execute(query)
        conn.commit()
        for row in cur:
            print(row)
        print("done") #debug

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    check_status(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()