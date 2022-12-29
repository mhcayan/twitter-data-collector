import logging
from Constants import *
from Helper import *
def test(a):
    logger1 = setup_logger(FILENAME.TEST_LOG_1.value, os.path.join(Constants.LOG_PATH.value, FILENAME.TEST_LOG_1.value + Extension.LOG.value))
    logger2 = setup_logger(FILENAME.TEST_LOG_2.value, os.path.join(Constants.LOG_PATH.value, FILENAME.TEST_LOG_2.value + Extension.LOG.value))
    logger1.info("this is info")
    logger1.info("this is debug")
    logger2.info("info message to logger2")
    print("test called")
    try:
        b = a / 0
    except Exception as e:
        logger1.exception("exception message")   
if __name__ == "__main__":
    test(99)