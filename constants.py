import os

from dotenv import load_dotenv

# list of youtuber names as they appear in their channels url handle
YTBRS_LIST = ["@caseoh_"]

CHANNEL_IDS_LIST = []


# crawler paths
CRAWLER_PATH = "./data/"
YOUTUBERS_PATH = CRAWLER_PATH + "youtubers.json"


# api key
load_dotenv()
# DEVELOPER_KEY = os.getenv("API_KEY")
# DEVELOPER_KEY = os.getenv("SECONDARY_API_KEY")
DEVELOPER_KEY = os.getenv("THIRD_API_KEY")
