import pytest
import unittest
import ssdetl.ssdetl
import datetime
import ConfigParser
import logging
from ssdetl.ssdetl import SSDEtl
from logging.config import fileConfig

fileConfig('logging.ini')
logger = logging.getLogger(__name__)

# Load the configuration file
conf = ConfigParser.ConfigParser()
conf.read('./config.ini')


class TestSsdEtl(unittest.TestCase):
    """Test suite for the ssdetl module.
    """
    etl = SSDEtl()
    logger.debug(etl.var)

    @unittest.skip
    def test_dbconnect(self):
        ssdetl.ssdetl.connectMySQL(conf)
        ssdetl.ssdetl.connectMongo(conf)

    @unittest.skip
    def test_fetchYT(self):
        db = ssdetl.ssdetl.connectMySQL(conf)
        data = ssdetl.ssdetl.fetchYT(
                db,
                datetime.datetime(2015, 7, 18),
                datetime.datetime(2015, 7, 19)
                )
        for row in data:
            logger.debug(row)

    @unittest.skip
    def test_get_start_end_date(self):
        enddate = ssdetl.ssdetl.lastEndDate(datetime.datetime(2015, 8, 20))
        print enddate
        startdate = ssdetl.ssdetl.lastStartDate()
        enddate = ssdetl.ssdetl.lastEndDate()
        # assert enddate.weekday() == 4
        print startdate
        print enddate
        self.assertEquals(enddate.weekday(), 4)
        # assert enddate.weekday() == 8
        # assert startdate.weekday() == 5
        self.assertEquals(startdate.weekday(), 5)
        # assert startdate < enddate
        self.assertLess(startdate, enddate)

    @unittest.skip
    def test_load_mongo(self):
        print 'test load mongo'
        dbmysql = ssdetl.ssdetl.connectMySQL(conf)
        data = ssdetl.ssdetl.fetchYT(
                dbmysql,
                datetime.datetime(2015, 7, 18),
                datetime.datetime(2015, 7, 22)
                )
        dbmongo = ssdetl.ssdetl.connectMongo(conf)
        ssdetl.ssdetl.loadYTToMongo(dbmongo, data)
        for row in dbmongo.days.find():
            # logger.debug('\n{}'.format(row))
            print row
