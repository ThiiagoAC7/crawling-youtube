import os

from dotenv import load_dotenv

# list of youtuber names as they appear in their channels url handle
YTBRS_LIST = []

CHANNEL_IDS_LIST = []


# crawler paths
CRAWLER_PATH = "./data/"
YOUTUBERS_PATH = CRAWLER_PATH + "youtubers.json"


# api keys
load_dotenv()
DEVELOPER_KEYS = [
    k
    for k in [
        os.getenv("API_KEY"),
        os.getenv("SECONDARY_API_KEY"),
        os.getenv("THIRD_API_KEY"),
    ]
    if k
]
