import ssdetl.ssdetl as ssd
from datetime import datetime
import ConfigParser
import logging
import pytz
from logging.config import fileConfig

# logging.basicConfig(level=logging.DEBUG)
fileConfig('logging.ini')
logger = logging.getLogger(__name__)

def main():
    
    # Set config file and logging
    conf = ConfigParser.ConfigParser()
    conf.read('config.ini')
    try:
        conf._sections.popitem()
    except KeyError:
        logger.critical('IOError: config.ini was not found')
        raise IOError

    # Set timezone info up
    tz = pytz.timezone('US/Pacific')

    # ssd.connectMongo(conf)
    # start_date = tz.normalize(
    #         tz.localize(datetime.datetime(2015, 7, 18))
    #         ).astimezone(pytz.utc)
    # end_date = tz.normalize(
    #         tz.localize(datetime.datetime(2015, 7, 19))
    #         ).astimezone(pytz.utc)

    db = ssd.connectMySQL(conf)
    data = ssd.fetchYT(
            db,
            datetime(2015, 7, 18),
            datetime(2015, 7, 19)
            )
    for row in range(0, 9):
        print data[row]

if __name__ == '__main__':
    main()
