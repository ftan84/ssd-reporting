import MySQLdb
import ConfigParser
import datetime
import pymongo
import logging

class SSDEtl:
    def __init__(self):
        self._var = 'hello world'

    @property
    def var(self):
        return self._var

logger = logging.getLogger(__name__)

def lastEndDate(date=datetime.datetime.now()):
    """Returns a datetime object of the most recent Friday.

    Example:
        If today is 2015-08-26, this function returns 2015-08-21
    """
    # now = datetime.datetime.now()
    date = date - datetime.timedelta(days=date.weekday()) + \
            datetime.timedelta(days=4, weeks=-1)
    date = date.date()
    date = datetime.datetime.combine(date, datetime.time(0))
    return date


def lastStartDate():
    """Returns a datetime object of the Saturday before lastEndDate()."""
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
    # Original query
    # query = '''
    #     SELECT A.day, C.id, C.name, A.views
    #     FROM yt_views A
    #     JOIN analytics_videos_bl_groups B ON A.analytics_video_id=B.analytics_video_id
    #     JOIN bl_groups C ON B.group_id=C.id
    #     WHERE A.day >= %s
    #     AND A.day <= %s
    #     GROUP BY A.day, C.id
    #     ORDER BY A.day, C.name
    #     '''
    query = '''
        SELECT
            ifnull(G.id, E.id) AS 'id',
            ifnull(G.name, E.name) AS 'showname',
            sum(A.views) AS 'views',
            sum(A.likes) AS 'likes',
            sum(A.dislikes) AS 'dislikes',
            sum(A.estimatedMinutesWatched) AS 'estimatedMinutesWatched',
            AVG(A.averageViewDuration) AS 'averageViewDuration',
            AVG(A.averageViewPercentage) AS 'averageViewPercentage',
            sum(A.favoritesAdded) AS 'favoritesAdded',
            sum(A.favoritesRemoved) AS 'favoritesRemoved',
            AVG(A.annotationCloseRate) AS 'annotationCloseRate',
            avg(A.annotationClickThroughRate) AS 'annotationClickThroughRate',
            sum(A.subscribersGained) AS 'subscribersGained',
            sum(A.subscribersLost) AS 'subscribersLost',
            sum(A.shares) AS 'shares',
            sum(A.comments) AS 'comments'
            FROM yt_views A
            LEFT JOIN analytics_videos_bl_groups B ON A.analytics_video_id = B.analytics_video_id
            LEFT JOIN bl_groups E ON B.group_id = E.id
            LEFT JOIN bl_groups_bl_groups F ON E.id = F.child_group_id
            LEFT JOIN bl_groups G ON F.parent_group_id = G.id
            WHERE A.day >= %s
            AND A.day <= %s
            AND (
                    (E.group_type_id = 1 AND G.group_type_id IS NULL)
                    OR
                    (E.group_type_id = 1 AND G.group_type_id = 3)
                    OR
                    (E.group_type_id = 2 AND G.group_type_id = 3)
                )
            GROUP BY showname
            ORDER BY showname
    '''

    db.execute(query, (start_date, end_date))
    data = db.fetchall()
    db.close()

    return data


def connectMongo(conf):
    """Establish connection to MongoDB based on the configuration file.

    Returns db connection.
    """
    section = 'Mongodb'
    logger.info('Starting connectMongo')
    mdbhost = conf.get(section, 'host')
    mdbport = conf.getint(section, 'port')
    mdbuser = conf.get(section, 'user')
    mdbpw = conf.get(section, 'pw')
    mddb = conf.get(section, 'db')
    client = pymongo.MongoClient(mdbhost, mdbport)
    client[mddb].authenticate(mdbuser, mdbpw)
    db = client[mddb]
    logger.info('Finished connectMongo')
    return db


def loadYTToMongo(db, data):
    """The function takes data taken from fetchYT and loads into Mongodb.
    """
    # Put all records into a dict first
    for row in data:
        date_entry = datetime.datetime.combine(
                row[0],
                datetime.time(0, 0, 0, 0))
        show_entry = {
                'show_id': row[1],
                'show_name': row[2],
                'youtube': {'views': row[3]}}


        db.days.update_one(
                {'timestamp': date_entry},
                {'$addToSet': {'shows': show_entry}},
                upsert=True)
