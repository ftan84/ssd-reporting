import pytest
import unittest
import ssdetl.ssdetl
import datetime
import ConfigParser
import logging
from logging.config import fileConfig

fileConfig('logging.ini')
logger = logging.getLogger(__name__)

# Load the configuration file
conf = ConfigParser.ConfigParser()
conf.read('./config.ini')


class TestSsdEtl(unittest.TestCase):

    def test_dbconnect(self):
        ssdetl.ssdetl.connectMySQL(conf)
        ssdetl.ssdetl.connectMongo(conf)

    # @pytest.skip
    def test_fetchYT():
        db = ssdetl.ssdetl.connectMySQL(conf)
        data = ssdetl.ssdetl.fetchYT(
                db,
                datetime.datetime(2015, 7, 18),
                datetime.datetime(2015, 7, 19)
                )
        testdata = [0, 30, 0, 93, 24, 1, 1, 0, 11, 2]
        for row in range(0, 9):
            assert data[row][3] == testdata[row]
            print data[row]


    def test_get_start_end_date(self):
        startdate = ssdetl.ssdetl.lastStartDate()
        enddate = ssdetl.ssdetl.lastEndDate()
        # assert enddate.weekday() == 4
        self.assertEquals(enddate.weekday(), 4)
        # assert enddate.weekday() == 8
        # assert startdate.weekday() == 5
        self.assertEquals(startdate.weekday(), 5)
        # assert startdate < enddate
        self.assertLess(startdate, enddate)

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
