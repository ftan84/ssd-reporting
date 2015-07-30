import MySQLdb
import ConfigParser
import datetime

def fetchdb():
    # Load the configuration file
    conf = ConfigParser.ConfigParser()
    conf.read('./config.cfg')
    dbhost = conf.get('Database', 'host')
    dbport = conf.getint('Database', 'port')
    dbuser = conf.get('Database', 'user')
    dbpw = conf.get('Database', 'pw')

    con = MySQLdb.connect(dbhost, port=dbport, user=dbuser,
                          passwd=dbpw, db='analytics')

    db = con.cursor()
    # SELECT A.day, C.id, C.name, A.views
    # FROM yt_views A
    # JOIN analytics_videos_bl_groups B ON A.analytics_video_id=B.analytics_video_id
    # JOIN bl_groups C ON B.group_id=C.id
    # WHERE A.day >= '2015-07-18'
    # AND A.day <= '2015-07-24'
    # GROUP BY A.day, C.id
    # ORDER BY A.day, C.name
    # limit 100
    query = '''
        SELECT A.day, C.id, C.name, A.views
        FROM yt_views A
        JOIN analytics_videos_bl_groups B ON A.analytics_video_id=B.analytics_video_id
        JOIN bl_groups C ON B.group_id=C.id
        WHERE A.day >= %s
        AND A.day <= %s
        GROUP BY A.day, C.id
        ORDER BY A.day, C.name
        limit 100
        '''
    db.execute(query, (datetime.datetime(2015, 6, 11), datetime.datetime(2015, 6, 13)))
    data = db.fetchall()
    for row in data:
        print row
    db.close()

def main():
    fetchdb()

if __name__ == '__main__':
    main()
