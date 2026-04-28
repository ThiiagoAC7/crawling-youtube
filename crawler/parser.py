import json

"""
parsing responses from youtube data api
"""


def _parse_json(request):
    return json.dumps(request, indent=4)


def save_data_to_json(data, path):
    print(f"Saving info at {path}")
    with open(path, "w") as f:
        f.write(_parse_json(data))


def parse_comment_threads(response, video_id, video_title, path):
    items = response["items"]
    comments_data = []
    comments_many_replies_ids = []  # comments with more than 5 replies

    for i in items:
        tlc = i["snippet"]["topLevelComment"]
        _comment = {}
        _comment["video_id"] = video_id
        _comment["video_title"] = video_title
        _comment["comment_id"] = tlc["id"]
        _comment["comment_text"] = (
            tlc["snippet"]["textDisplay"]
            .replace("\r\n", " ")
            .replace("\n", " ")
            .replace("\r", " ")
            .strip()
        )
        # _comment["comment_author_name"] = tlc["snippet"]["authorDisplayName"]
        _comment["comment_author_channel_id"] = tlc["snippet"]["authorChannelId"][
            "value"
        ]
        _comment["comment_like_count"] = tlc["snippet"]["likeCount"]
        _comment["comment_publish_date"] = tlc["snippet"]["publishedAt"]
        _comment["comment_reply_count"] = i["snippet"]["totalReplyCount"]
        _comment["is_reply"] = False
        _comment["parent_comment_id"] = 0

        comments_data.append(_comment)

        reply_count = int(i["snippet"]["totalReplyCount"])
        replies = i.get("replies")
        if replies and reply_count > 0 and reply_count <= 5:  # pra economizar requests
            comments_data += parse_replies(replies, tlc["id"], video_id, video_title)
        elif reply_count > 5:
            comments_many_replies_ids.append(tlc["id"])

    return comments_data, comments_many_replies_ids


def parse_replies(replies, parent_id, video_id, video_title, many=False):
    subcomments = replies.get("comments")
    if many:
        subcomments = replies.get("items")

    subcomments_data = []

    for s in subcomments:
        _subcomment = {}
        _subcomment["video_id"] = video_id
        _subcomment["video_title"] = video_title
        _subcomment["comment_id"] = s["id"]
        _subcomment["comment_text"] = (
            s["snippet"]["textDisplay"]
            .replace("\r\n", " ")
            .replace("\n", " ")
            .replace("\r", " ")
            .strip()
        )
        # _subcomment["comment_author_name"] = s["snippet"]["authorDisplayName"]
        _subcomment["comment_author_channel_id"] = s["snippet"]["authorChannelId"][
            "value"
        ]
        _subcomment["comment_like_count"] = s["snippet"]["likeCount"]
        _subcomment["comment_publish_date"] = s["snippet"]["publishedAt"]
        _subcomment["comment_reply_count"] = 0
        _subcomment["is_reply"] = True
        _subcomment["parent_comment_id"] = parent_id

        subcomments_data.append(_subcomment)

    return subcomments_data


def parse_search_videos(response, channel, path):
    videos_data = {}
    videos_data["channel_title"] = channel["channel_title"]
    videos_data["channel_id"] = channel["channel_id"]
    videos_data["youtuber"] = channel["youtuber"]
    videos_data["nextPageToken"] = response["nextPageToken"]
    videos_data["videos"] = []
    items = response["items"]

    for i in items:
        _video_info = {}
        _video_info["video_id"] = i["id"]["videoId"]
        _snippets = i["snippet"]
        _video_info["date_published"] = _snippets["publishedAt"]
        _video_info["video_title"] = _snippets["title"]
        _video_info["video_desc"] = _snippets["description"]
        videos_data["videos"].append(_video_info)

    _path = f"{path}/videos_list.json"
    save_data_to_json(videos_data, _path)


def parse_channel_info(response):
    # get response specific infos
    items = response["items"][0]

    _id = items["id"]
    _title = items["snippet"]["title"]
    _videos_id = items["contentDetails"]["relatedPlaylists"]["uploads"]
    _subs_count = items["statistics"]["subscriberCount"]
    _video_count = items["statistics"]["videoCount"]
    _view_count = items["statistics"]["viewCount"]
    _name = items["snippet"]["customUrl"]

    ytbr_data = {
        "youtuber": _name,
        "channel_id": _id,
        "channel_title": _title,
        "uploaded_videos_id": _videos_id,
        "num_subs": _subs_count,
        "num_videos": _video_count,
        "num_views": _view_count,
    }

    return ytbr_data
