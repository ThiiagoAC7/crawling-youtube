import json
import os
from datetime import datetime, timezone

import googleapiclient.errors
import pandas as pd

from constants import (
    CHANNEL_IDS_LIST,
    CRAWLER_PATH,
    DEVELOPER_KEYS,
    YOUTUBERS_PATH,
    YTBRS_LIST,
)

from .api_manager import YouTubeAPIManager
from .parser import *


class Crawling:
    def __init__(
        self, channel_ids=None, youtubers=None, api_keys=None, output_dir=None
    ):
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "0"

        self.yt_channel_ids = []
        self.channel_ids = channel_ids if channel_ids is not None else CHANNEL_IDS_LIST
        self.youtubers = youtubers if youtubers is not None else YTBRS_LIST
        self.api_keys = api_keys if api_keys is not None else DEVELOPER_KEYS
        self.crawler_path = output_dir if output_dir is not None else CRAWLER_PATH
        self.youtubers_path = self.crawler_path + "youtubers.json"

        if not os.path.exists(self.crawler_path):
            os.makedirs(self.crawler_path)

        self.api_manager = YouTubeAPIManager(self.api_keys)

    ##
    # CHANNELS LIST
    ##

    def build_channels_list(self):
        """
        builds youtubers_channel_list dataset
        """
        youtubers = []
        for name in self.youtubers:
            print(f"Crawling info from @{name} ...")

            method_func = lambda client, **kw: client.channels().list(**kw)
            response = self.api_manager.make_request(
                method_func,
                part="snippet,contentDetails,statistics",
                forHandle=name,
            )

            ytbr_data = parse_channel_info(response)
            youtubers.append(ytbr_data)

        print(f"got channels info. saving at {self.youtubers_path}")
        save_data_to_json(youtubers, self.youtubers_path)

    ##
    # CHANNEL LIST FROM ID
    ##
    def build_channels_list_from_id(self):
        """
        builds youtubers_channel_list dataset
        """
        youtubers = []
        for id in self.channel_ids:
            print(f"Crawling info from @{id} ...")

            method_func = lambda client, **kw: client.channels().list(**kw)
            response = self.api_manager.make_request(
                method_func,
                part="snippet,contentDetails,statistics",
                id=id,
            )

            print(response)

            if response["pageInfo"]["totalResults"] > 0:
                ytbr_data = parse_channel_info(response)
                youtubers.append(ytbr_data)

        print(f"got channels info. saving at {self.youtubers_path}")
        save_data_to_json(youtubers, self.youtubers_path)

    ##
    # VIDEOS LIST
    ##

    def build_youtubers_videos_list(self):
        """
        builds youtubers_videos_list json dataset
        with latest videos_data for each youtuber specified
        """
        if not os.path.exists(self.youtubers_path):
            self.build_channels_list()

        youtubers_list = []
        with open(self.youtubers_path) as f:
            youtubers_list = json.load(f)

        for channel in youtubers_list:
            print(
                f"Crawling info from : {channel['channel_title']}, @{channel['youtuber']} ..."
            )

            method_func = lambda client, **kw: client.search().list(**kw)
            response = self.api_manager.make_request(
                method_func,
                part="snippet",
                channelId=channel["channel_id"],
                order="date",
                type="video",
                maxResults=50,
            )

            _path = self.crawler_path + channel["youtuber"]
            os.makedirs(_path, exist_ok=True)
            parse_search_videos(response, channel, _path)

    ##
    # COMMENTS DF
    ##

    def build_videos_comments_df(self, limit=50):
        datasets = self._get_youtuber_datasets_path()

        # get each youtuber's videos dataset
        for path in datasets:
            video_data = []
            video_data_path = path + "videos_list.json"
            with open(video_data_path, "r") as f:
                video_data = json.load(f)

            # crawl manually each youtuber, to check api limits
            # manual = ["@TheTrenchFamily"]
            # if video_data["youtuber"] in manual:
            # manual = []

            print(f"crawling comments from @{video_data['youtuber']}'s videos")
            self._get_comments_from_video_ids(
                video_data,
                video_data_path,
                path,
                limit=limit,
            )

    def _get_youtuber_datasets_path(self):
        """
        returns all youtuber datasets path as a list
        """
        data = []

        # getting youtuber folders
        for item in os.listdir(self.crawler_path):
            _item_path = os.path.join(self.crawler_path, item)
            if os.path.isdir(_item_path):
                data.append(_item_path + "/")
        return data

    def _get_replies_from_parent_ids(
        self, parent_ids, video_id, video_title
    ) -> pd.DataFrame:
        """
        gets replies from comments with more than 5 replies
        - commentThread endpoint only returns 5 replies per comment!
        """
        page_token = None
        _count = 0
        df = pd.DataFrame()

        for id in parent_ids:
            while True:
                _count += 1
                method_func = lambda client, **kw: client.comments().list(**kw)
                try:
                    response = self.api_manager.make_request(
                        method_func,
                        part="snippet,id",
                        maxResults=100,
                        pageToken=page_token,
                        parentId=id,
                        textFormat="plainText",
                    )
                except googleapiclient.errors.HttpError as e:
                    if e.error_details[0]["reason"] == "commentNotFound":
                        print(
                            f"skipping current comment: {e.error_details[0]['message']}"
                        )
                        break
                    raise

                if response:
                    print(f"\t\tparsing comment replies ... {_count}")
                    _d = parse_replies(
                        response,
                        id,
                        video_id,
                        video_title,
                        many=True,
                    )
                    df = pd.concat([df, pd.DataFrame(_d)], ignore_index=True)
                page_token = response.get("nextPageToken")
                if not page_token:
                    break

        return df

    def _get_comments_from_video_ids(
        self, video_data, video_data_path, path, limit=50, filter_ids=[]
    ):
        """
        iterates through each video, gets its comments and saves dataset.
        Params:
        - videos: videos list, with video_id, date, video_title
        - path: current youtuber path, i.e ./data/{youtuber}/
        """
        df = pd.DataFrame()
        page_token = None  # video's comment section has many pages

        print(f"Collecting the first {limit} videos (if not collected already)")

        num_requests = 0
        num_comments_count = 0
        for v in video_data["videos"][:limit]:  # first 50 videos
            _count = 0

            if v["collected"]:  # skip collected vids
                continue

            if (filter_ids) and (v["video_id"] not in filter_ids):
                print(f"not in selected videos:{v['video_id']}")
                continue

            if int(v["comment_count"]) == 0:
                print("Skipping video, 0 comments ...")
                v["collected"] = True
                continue

            print(f"\tcomments from {v['video_id']}, {v['video_title']}")
            while True:  # to get next pages if nextPageToken != None
                _count += 1
                method_func = lambda client, **kw: client.commentThreads().list(**kw)
                try:
                    response = self.api_manager.make_request(
                        method_func,
                        part="snippet,replies,id",
                        videoId=v["video_id"],
                        maxResults=100,
                        pageToken=page_token,
                        order="time",
                        textFormat="plainText",
                    )
                    num_requests += 1
                except googleapiclient.errors.HttpError as e:
                    if e.error_details[0]["reason"] == "commentsDisabled":
                        print(
                            f"skipping current video: {e.error_details[0]['message']}"
                        )
                        break
                    raise

                if response:
                    print(
                        f"\t\tparsing comments and appending to dataframe, page {_count}, req {num_requests}"
                    )
                    _d, comments_many_replies_ids = parse_comment_threads(
                        response, v["video_id"], v["video_title"], video_data_path
                    )
                    df = pd.concat([df, pd.DataFrame(_d)], ignore_index=True)
                    if comments_many_replies_ids != []:
                        # commentThread endpoint only returns 5 replies per comment!
                        repl_df = self._get_replies_from_parent_ids(
                            comments_many_replies_ids,
                            v["video_id"],
                            v["video_title"],
                        )
                        df = pd.concat([df, repl_df], ignore_index=True)

                    num_comments_count = len(df)

                page_token = response.get("nextPageToken")
                if not page_token:  # if next comment page doesnt exist, break
                    break

            # update json
            v["collected"] = True
            with open(video_data_path, "w") as f:
                json.dump(video_data, f, indent=4)
            print(
                f"\tprogress saved. '{v['video_title']}' marked as collected. Currently: {num_comments_count} comments."
            )

        print(f"saving...")
        df.to_csv(f"{path}comments_0_{limit}_new.csv")

    ##
    # UPLOADS WITHOUT SHORTS
    ##

    def _get_uploads_id(self, channel):
        method_func = lambda client, **kw: client.channels().list(**kw)
        response = self.api_manager.make_request(
            method_func,
            part="contentDetails",
            id=channel["channel_id"],
        )

        return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    def _get_video_ids_from_playlist(self, playlist_id):
        video_ids = []
        next_page_token = None
        while True:
            method_func = lambda client, **kw: client.playlistItems().list(**kw)
            response = self.api_manager.make_request(
                method_func,
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )

            video_ids.extend(
                [item["contentDetails"]["videoId"] for item in response["items"]]
            )
            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                break

        return video_ids

    def _get_video_details(self, video_ids):
        videos = []

        start_date_limit = datetime(
            2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc
        )  # 01 jan 24
        end_date_limit = datetime(
            2024, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc
        )  # 31 dez 24

        counter = 0
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]

            method_func = lambda client, **kw: client.videos().list(**kw)
            response = self.api_manager.make_request(
                method_func,
                part="snippet,contentDetails,statistics",
                id=",".join(batch),
            )

            for item in response.get("items", []):
                # duration = parse_duration(item['contentDetails']['duration']).total_seconds()

                # adjust according to ytbs (lives and shorts)
                # duration_filter = duration >= 0 and duration <= 99999
                duration_filter = True

                # filter video to the year 2024
                video_published_date = datetime.fromisoformat(
                    item["snippet"]["publishedAt"][:-1] + "+00:00"
                )
                date_filter = start_date_limit <= video_published_date <= end_date_limit
                # date_filter = True

                if date_filter and duration_filter:  # filter only videos of 2024
                    videos.append(
                        {
                            "video_id": item["id"],
                            "date_published": item["snippet"]["publishedAt"],
                            "video_title": item["snippet"]["title"],
                            "video_desc": item["snippet"].get("description", ""),
                            "view_count": item["statistics"].get("viewCount", "0"),
                            "like_count": item["statistics"].get("likeCount", "0"),
                            "comment_count": item["statistics"].get(
                                "commentCount", "0"
                            ),
                            "collected": False,
                            "idx": counter,
                        }
                    )
                    counter += 1

        return videos

    def build_youtubers_videos_list_from_uploads(self):
        if not os.path.exists(self.youtubers_path):
            self.build_channels_list()

        youtubers_list = []
        with open(self.youtubers_path) as f:
            youtubers_list = json.load(f)

        for channel in youtubers_list[9:10]:  # 3:4 zackd
            print(
                f"Crawling info from: {channel['channel_title']}, {channel['youtuber']}..."
            )
            try:
                upload_id = self._get_uploads_id(channel)
                print(f"got upload id {upload_id}")
                video_ids = self._get_video_ids_from_playlist(upload_id)
                print(f"got video ids ... {len(video_ids)} videos")
                videos = self._get_video_details(video_ids)
                print(f"got uploads ... {len(videos)} videos")

                channel_data = {
                    "channel_title": channel["channel_title"],
                    "channel_id": channel["channel_id"],
                    "youtuber": channel["youtuber"],
                    "videos": videos,
                }

                _path = self.crawler_path + channel["youtuber"]
                os.makedirs(_path, exist_ok=True)
                with open(f"{_path}/videos_list.json", "w") as f:
                    json.dump(channel_data, f, indent=4)

                print(f"saved at {_path}")

            except Exception as e:
                print(f"Error processing {channel['channel_title']}: {str(e)}")
                continue
