import argparse
import json

import pandas as pd

from utils import get_youtubers


def verify_videos(yt):

    with open(f"./data/{yt}/videos_list.json", "r") as f:
        dados = json.load(f)

    df = pd.read_csv(f"./data/{yt}/comments.csv")
    video_ids = df["video_id"].unique()
    del df

    not_collected_ids = []
    _not_collected_no_comments = []
    n = 1
    for _, video in enumerate(dados.get("videos", [])):
        if video["video_id"] in video_ids:
            print(f"{n} -  id: {video['video_id']}, index: {video['idx']}  ")
            print(f"\t views: {video['view_count']}")
            print(f"\t comments: {video['comment_count']}")
            print(f"\t collected: {video['collected']}")
        else:
            print(f"{video['video_id']} not collected !")
            not_collected_ids.append(video["video_id"])
            if video["comment_count"] == "0":
                _not_collected_no_comments.append(video["video_id"])

        n += 1

    print(f"numero de videos: {n}")
    # print(f"not collected vids: {not_collected_ids}")
    print(f"not collected -> 0 comments : {len(_not_collected_no_comments)}")
    print(f"not collected ->  : {len(set(not_collected_ids) - set(_not_collected_no_comments))}")


    with open(f"./data/{yt}/to_collect.txt", "w") as f:
        f.write("\n".join(not_collected_ids))


def main():
    parser = argparse.ArgumentParser(description="Verify collected videos")
    parser.add_argument(
        "--youtubers",
        nargs="+",
        help="list of space separated youtuber handles (e.g. @caseoh_ @mrbeast)",
    )
    args = parser.parse_args()

    youtubers = get_youtubers(args.youtubers)
    for yt in youtubers:
        print(f"\n=== Verifying {yt} ===")
        verify_videos(f"@{yt}")


if __name__ == "__main__":
    main()
