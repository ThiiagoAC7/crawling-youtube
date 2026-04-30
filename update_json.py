import argparse
import json

import pandas as pd

from utils import get_youtubers


def update_json(yt):
    path = f"./data/{yt}/comments.csv"

    df = pd.read_csv(path)
    video_ids = df["video_id"].unique()

    with open(f"./data/{yt}/videos_list.json", "r") as f:
        dados = json.load(f)

    for indice, video in enumerate(dados.get("videos", [])):
        if (video["video_id"] in video_ids) or (video["comment_count"] == "0"):
            video["collected"] = True
        else:
            video["collected"] = False

    with open(f"./data/{yt}/videos_list.json", "w") as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)


def main():
    parser = argparse.ArgumentParser(
        description="Update videos_list.json with collected status"
    )
    parser.add_argument(
        "--youtubers",
        nargs="+",
        help="list of space separated youtuber handles (e.g. @caseoh_ @mrbeast)",
    )
    args = parser.parse_args()

    youtubers = get_youtubers(args.youtubers)
    for yt in youtubers:
        print(f"\n=== Updating {yt} ===")
        update_json(f"@{yt}")


if __name__ == "__main__":
    main()
