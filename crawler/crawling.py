import json
import os
from datetime import datetime, timezone

import googleapiclient.errors
import isodate
import pandas as pd

from constants import (
    CHANNEL_IDS_LIST,
    CRAWLER_PATH,
    DEVELOPER_KEYS,
    YOUTUBERS_PATH,
    YTBRS_LIST,
)

from .api_manager import QuotaExhaustedError, YouTubeAPIManager
from .parser import *
from .progress import VideoProgress


class Crawling:
    SAVE_EVERY_N_VIDEOS = 100

    def __init__(
        self,
        channel_ids=None,
        youtubers=None,
        api_keys=None,
        output_dir=None,
        filters=None,
    ):
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "0"

        self.yt_channel_ids = []
        self.channel_ids = channel_ids if channel_ids is not None else CHANNEL_IDS_LIST
        self.youtubers = youtubers if youtubers is not None else YTBRS_LIST
        self.api_keys = api_keys if api_keys is not None else DEVELOPER_KEYS
        self.crawler_path = output_dir if output_dir is not None else CRAWLER_PATH
        self.youtubers_path = self.crawler_path + "youtubers.json"

        _filters = filters or {}
        self.start_date = self._parse_date(_filters.get("start_date"), "2005-01-01")
        self.end_date = self._parse_date(_filters.get("end_date"), None)
        self.min_duration = _filters.get("min_duration")
        self.max_duration = _filters.get("max_duration")

        if not os.path.exists(self.crawler_path):
            os.makedirs(self.crawler_path)

        self.api_manager = YouTubeAPIManager(self.api_keys)

    def _parse_date(self, date_str, default):
        """
        parses a date string in YYYY-MM-DD format.
        if default is None, returns current utc datetime.
        """
        if date_str:
            return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if default is None:
            return datetime.now(timezone.utc)
        return datetime.strptime(default, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    def _merge_and_save_youtubers(self, new_youtubers):
        """
        merges new_youtubers into existing youtubers.json,
        deduplicating by channel_id.
        """
        existing = []
        if os.path.exists(self.youtubers_path):
            with open(self.youtubers_path, "r", encoding="utf-8") as f:
                existing = json.load(f)

        seen = {ch["channel_id"] for ch in existing}
        merged = existing.copy()
        for ch in new_youtubers:
            if ch["channel_id"] not in seen:
                merged.append(ch)
                seen.add(ch["channel_id"])

        save_data_to_json(merged, self.youtubers_path)

    def _all_channels_present(self, identifiers, key_field):
        """
        checks if youtubers.json exists and contains all identifiers.

        params:
        - identifiers: list of identifiers to check (handles or channel ids)
        - key_field: field in youtubers.json to match against

        returns: True if all present, False if build needed
        """
        if not identifiers:
            return True

        if not os.path.exists(self.youtubers_path):
            return False

        with open(self.youtubers_path, "r", encoding="utf-8") as f:
            existing = json.load(f)

        existing_values = {ch[key_field][1:] for ch in existing}
        print(identifiers)
        print(existing_values)
        return set(identifiers).issubset(existing_values)

    ##
    # CHANNELS LIST
    ##

    def build_channels_list(self):
        """
        builds youtubers_channel_list dataset
        """
        if self._all_channels_present(self.youtubers, "youtuber"):
            print(f" all channels are present on youtubers.json ...")
            return


        youtubers = []
        print(self.youtubers)
        for name in self.youtubers:
            print(f"Crawling info from @{name} ...")

            method_func = lambda client, **kw: client.channels().list(**kw)
            response = self.api_manager.make_request(
                method_func, part="snippet,contentDetails,statistics", forHandle=name
            )

            if response.get("items"):
                ytbr_data = parse_channel_info(response)
                youtubers.append(ytbr_data)
            else:
                print(response)
                # todo: handle error

        print(f"got channels info. saving at {self.youtubers_path}")
        self._merge_and_save_youtubers(youtubers)

    ##
    # CHANNEL LIST FROM ID
    ##
    def build_channels_list_from_id(self):
        """
        builds youtubers_channel_list dataset
        """
        if self._all_channels_present(self.channel_ids, "channel_id"):
            return

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
        self._merge_and_save_youtubers(youtubers)

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

    def build_videos_comments_df(self, limit=10000, filter=[]):
        datasets = self._get_youtuber_datasets_path(filter)

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

    def _get_youtuber_datasets_path(self, filter=[]):
        """
        returns all youtuber datasets path as a list
        """
        data = []

        filter = [s.lower() for s in filter]  # for str comparison

        # getting youtuber folders
        for item in os.listdir(self.crawler_path):
            if not item.startswith("@"):
                continue

            # remove @ from item path
            if filter and item[1:] not in filter:
                continue

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
        df = pd.DataFrame()

        for id in parent_ids:
            while True:
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

    def _save_comments(
        self, df, path, partial=False, error=False, collected_video_ids=None
    ):
        """
        saves comments dataframe with timestamped filename.

        params:
        - df: comments dataframe
        - path: youtuber directory path (e.g., ./data/@mrbeast/)
        - partial: if True, appends '_partial' suffix
        - error: if True, appends '_error' suffix
        - collected_video_ids: list of video ids to mark as collected in videos_list.json

        returns: full filepath of saved file
        """
        now = datetime.now()
        ts = f"{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}_{now.second}"
        if error:
            suffix = "_error"
        elif partial:
            suffix = "_partial"
        else:
            suffix = ""
        filename = f"comments_{ts}{suffix}.csv"
        filepath = os.path.join(path, filename)
        df.to_csv(filepath, index=False)

        if collected_video_ids:
            json_path = os.path.join(path, "videos_list.json")
            if os.path.exists(json_path):
                with open(json_path, "r") as f:
                    video_data = json.load(f)
                for v in video_data["videos"]:
                    if v["video_id"] in collected_video_ids:
                        v["collected"] = True
                with open(json_path, "w") as f:
                    json.dump(video_data, f, indent=4)
            collected_video_ids.clear()

        return filepath

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

        tracker = VideoProgress(video_data["youtuber"], path)
        tracker.log_header(limit)

        collected_ids = []
        try:
            for idx, v in enumerate(video_data["videos"][:limit], start=1):

                if v["collected"]:  # skip collected vids
                    continue

                if (filter_ids) and (v["video_id"] not in filter_ids):
                    print(f"not in selected videos:{v['video_id']}")
                    continue

                if int(v["comment_count"]) == 0:
                    print("Skipping video, 0 comments ...")
                    collected_ids.append(v["video_id"])
                    continue

                tracker.set_video(v["video_id"], v["video_title"])
                while True:  # to get next pages if nextPageToken != None
                    method_func = lambda client, **kw: client.commentThreads().list(
                        **kw
                    )
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
                    except googleapiclient.errors.HttpError as e:
                        if e.error_details[0]["reason"] == "commentsDisabled":
                            print(
                                f"skipping current video: {e.error_details[0]['message']}"
                            )
                            break
                        tracker.save_log(collected_ids, label="error")
                        self._save_comments(
                            df, path, error=True, collected_video_ids=collected_ids
                        )
                        raise

                    if response:
                        _d, comments_many_replies_ids = parse_comment_threads(
                            response, v["video_id"], v["video_title"], video_data_path
                        )
                        df = pd.concat([df, pd.DataFrame(_d)], ignore_index=True)
                        tracker.add_comments(len(_d))
                        if comments_many_replies_ids != []:
                            # commentThread endpoint only returns 5 replies per comment!
                            repl_df = self._get_replies_from_parent_ids(
                                comments_many_replies_ids,
                                v["video_id"],
                                v["video_title"],
                            )
                            df = pd.concat([df, repl_df], ignore_index=True)
                            tracker.add_replies(len(repl_df))

                    page_token = response.get("nextPageToken")
                    if not page_token:  # if next comment page doesnt exist, break
                        break

                collected_ids.append(v["video_id"])
                tracker.video_done()

                if idx % self.SAVE_EVERY_N_VIDEOS == 0:
                    tracker.save_log(collected_ids, label="partial")
                    saved_path = self._save_comments(
                        df, path, partial=True, collected_video_ids=collected_ids
                    )
                    print(f"progress saved after {idx} videos. file: {saved_path}")
                    df = pd.DataFrame()

            print("saving...")
            tracker.save_log(collected_ids, label="final")
            saved_path = self._save_comments(
                df, path, collected_video_ids=collected_ids
            )
            print(f"saved to: {saved_path}")

        except QuotaExhaustedError:
            print("\n" + "=" * 50)
            print("all api keys exhausted. saving current progress...")
            tracker.save_log(collected_ids, label="quota_exhausted")
            partial_path = self._save_comments(
                df, path, partial=True, collected_video_ids=collected_ids
            )
            print(f"partial progress saved to: {partial_path}")

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
                duration = isodate.parse_duration(
                    item["contentDetails"]["duration"]
                ).total_seconds()

                # filters by video length
                if self.min_duration is not None and duration < self.min_duration:
                    continue
                if self.max_duration is not None and duration > self.max_duration:
                    continue

                video_published_date = datetime.fromisoformat(
                    item["snippet"]["publishedAt"][:-1] + "+00:00"
                )

                # filters by publish date
                if not (self.start_date <= video_published_date <= self.end_date):
                    continue

                videos.append(
                    {
                        "video_id": item["id"],
                        "date_published": item["snippet"]["publishedAt"],
                        "video_title": item["snippet"]["title"],
                        "video_desc": item["snippet"].get("description", ""),
                        "view_count": item["statistics"].get("viewCount", "0"),
                        "like_count": item["statistics"].get("likeCount", "0"),
                        "comment_count": item["statistics"].get("commentCount", "0"),
                        "collected": False,
                        "idx": counter,
                    }
                )
                counter += 1

        return videos

    def build_youtubers_videos_list_from_uploads(self, force=False):
        if not os.path.exists(self.youtubers_path):
            self.build_channels_list()

        youtubers_list = []
        with open(self.youtubers_path) as f:
            youtubers_list = json.load(f)

        for channel in youtubers_list:
            _path = self.crawler_path + channel["youtuber"]
            _json_path = f"{_path}/videos_list.json"

            if not force and os.path.exists(_json_path):
                print(f"{_json_path} already exists and force = {force}, skipping ...")
                continue

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

                os.makedirs(_path, exist_ok=True)
                with open(_json_path, "w") as f:
                    json.dump(channel_data, f, indent=4)

                print(f"saved at {_path}")

            except Exception as e:
                print(f"Error processing {channel['channel_title']}: {str(e)}")
                continue
