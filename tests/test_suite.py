import pytest
import ssdetl.ssdetl
import datetime
import ConfigParser

# Load the configuration file
conf = ConfigParser.ConfigParser()
conf.read('./config.cfg')


def test_dbconnect():
    ssdetl.ssdetl.connectMySQL(conf)
    ssdetl.ssdetl.connectMongo()


@pytest.skip
def test_db():
    db = ssdetl.ssdetl.connectMySQL(conf)
    data = ssdetl.ssdetl.fetchYT(
            db,
            datetime.datetime(2015, 7, 18),
            datetime.datetime(2015, 7, 19)
            )
    testdata = [0, 30, 0, 93, 24, 1, 1, 72, 11, 2]
    for row in range(0, 9):
        assert data[row][3] == testdata[row]


def test_get_start_end_date():
    startdate = ssdetl.ssdetl.lastStartDate()
    enddate = ssdetl.ssdetl.lastEndDate()
    assert enddate.weekday() == 4
    assert startdate.weekday() == 5
    assert startdate < enddate
