import MySQLdb
import ConfigParser
import datetime
import pymongo
import logging

logger = logging.getLogger(__name__)

def lastEndDate():
    now = datetime.datetime.now()
    return now - datetime.timedelta(days=now.weekday()) + \
            datetime.timedelta(days=4, weeks=-1)


def lastStartDate():
    enddate = lastEndDate()
    return enddate - datetime.timedelta(days=6)


def connectMySQL(conf):
    section = 'MySQL'
    dbhost = conf.get(section, 'host')
    dbport = conf.getint(section, 'port')
    dbuser = conf.get(section, 'user')
    dbpw = conf.get(section, 'pw')

    con = MySQLdb.connect(dbhost, port=dbport, user=dbuser,
                          passwd=dbpw, db='analytics')
    db = con.cursor()
    return db


def fetchYT(db, start_date=datetime.datetime(2015, 1, 1),
        end_date=datetime.datetime(2015, 1, 8)
        ):

    logger.debug('Starting fetchYT')
    query = '''
        SELECT A.day, C.id, C.name, A.views
        FROM yt_views A
        JOIN analytics_videos_bl_groups B ON A.analytics_video_id=B.analytics_video_id
        JOIN bl_groups C ON B.group_id=C.id
        WHERE A.day >= %s
        AND A.day <= %s
        GROUP BY A.day, C.id
        ORDER BY A.day, C.name
        '''
    db.execute(query, (start_date, end_date))
    data = db.fetchall()
    db.close()

    return data


def connectMongo(conf):
    section = 'Mongodb'
    logger.info('Starting connectMongo')
    mdbhost = conf.get(section, 'host')
    mdbport = conf.getint(section, 'port')
    mdbuser = conf.get(section, 'user')
    mdbpw = conf.get(section, 'pw')
    client = pymongo.MongoClient(mdbhost, mdbport)
    client['ssdreporting'].authenticate(mdbuser, mdbpw)
    db = client['ssdreporting']
    logger.info('Finished connectMongo')
    return db


def loadMongo(db, data):
    """
    days
    {
        timestamp: ISO,
        views: total views,
        shows: [
            {
                show: Ref to show,
                views: views
            }, {...}
        ]
    }
    """
    print 'hello'
