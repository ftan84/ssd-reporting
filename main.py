import ssdetl.ssdetl as ssd
import datetime
import ConfigParser
import logging
from logging.config import fileConfig

# logging.basicConfig(level=logging.DEBUG)
fileConfig('logging.ini')
logger = logging.getLogger(__name__)

def main():
    logger.debug('start main')
    conf = ConfigParser.ConfigParser()
    conf.read('config.ini')
    ssd.connectMongo(conf)
    # db = ssd.connectMySQL(conf)
    # data = ssd.fetchYT(
    #         db,
    #         datetime.datetime(2015, 7, 18),
    #         datetime.datetime(2015, 7, 19)
    #         )
    # for row in range(0, 9):
    #     print data[row]

if __name__ == '__main__':
    main()
