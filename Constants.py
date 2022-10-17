from enum import Enum
import os
import re
class Constants(Enum):

    PROJECT_PATH = r"F:\E\code\twitter-data-collector"
    RESOURES_PATH = os.path.join(PROJECT_PATH, "resources")

class Extension(Enum):
    XLSX = ".xlsx"
    CSV = ".csv"

class FILENAME(Enum):
    INPUT = "20220909_input_file_natalie"
    SAMPLE_INPUT = "sample_input"
    GOOGLE_SEARCH = "google_search_result"

class PATTERN(Enum):
    PROFILE_URL = re.compile(".*.twitter.com/([^/]*)/?.*")
    STATUS_URL = re.compile(".*.twitter.com/.*/status/([\d+]*)\??.*")