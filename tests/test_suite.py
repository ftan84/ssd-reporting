# import pytest
import unittest
import utilities.etl
import datetime
import ConfigParser
import logging
from utilities.etl import Processor
from logging.config import fileConfig

fileConfig('logging.ini')
logger = logging.getLogger(__name__)

# Load the configuration file
conf = ConfigParser.ConfigParser()
conf.read('config.ini')


class TestETL(unittest.TestCase):
    """Test suite for the ssdetl module.
    """
    def setUp(self):
        self.etl = utilities.etl.Processor(conf)


    def test_instatiation(self):
        etl = utilities.etl.Processor(conf)
        section = 'MySQL'
        self.assertEquals(etl.conf.get(section, 'host'), '127.0.0.1')
        self.assertEquals(etl.conf.get(section, 'port'), '3308')
        self.assertEquals(etl.conf.get(section, 'user'), 'ftan')
        section = 'Mongodb'
        self.assertEquals(etl.conf.get(section, 'host'), '127.0.0.1')
        self.assertEquals(etl.conf.get(section, 'port'), '27017')
        self.assertEquals(etl.conf.get(section, 'user'), 'ssdreportingUser')
        self.assertEquals(etl.conf.get(section, 'db'), 'ssdreporting')


    def test_get_start_end_date(self):
        # etl = utilities.etl.Processor(conf)
        enddate = self.etl._lastEndDate(datetime.datetime(2015, 8, 20))
        logger.debug(enddate)
        startdate = self.etl._lastStartDate()
        enddate = self.etl._lastEndDate()
        # assert enddate.weekday() == 4
        logger.debug(startdate)
        logger.debug(enddate)
        self.assertEquals(enddate.weekday(), 4)
        # assert enddate.weekday() == 8
        # assert startdate.weekday() == 5
        self.assertEquals(startdate.weekday(), 5)
        # assert startdate < enddate
        self.assertLess(startdate, enddate)


    def test_dbconnect(self):
        logger.debug(self.etl.conf.get('MySQL', 'host'))
        self.etl._connectMySQL()
        # self.etl.connectMongo(conf)

    @unittest.skip
    def test_fetchYT(self):
        db = utilities.etl.connectMySQL(conf)
        data = utilities.etl.fetchYT(
                db,
                datetime.datetime(2015, 7, 18),
                datetime.datetime(2015, 7, 19)
                )
        for row in data:
            logger.debug(row)


    @unittest.skip
    def test_load_mongo(self):
        print 'test load mongo'
        dbmysql = utilities.etl.connectMySQL(conf)
        data = utilities.etl.fetchYT(
                dbmysql,
                datetime.datetime(2015, 7, 18),
                datetime.datetime(2015, 7, 22)
                )
        dbmongo = utilities.etl.connectMongo(conf)
        utilities.etl.loadYTToMongo(dbmongo, data)
        for row in dbmongo.days.find():
            # logger.debug('\n{}'.format(row))
            print row
