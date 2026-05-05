import argparse
import json

import pandas as pd

from utils import get_youtubers


def check_comments(yt, mark_mismatches=False):
    path = f"./data/@{yt}"

    with open(f"{path}/videos_list.json", "r") as f:
        data = json.load(f)

    df = pd.read_csv(f"{path}/comments.csv")

    actual_counts = df.groupby("video_id").size().to_dict()

    print(f"\n{'video_id':<15} {'expected':>10} {'actual':>10} {'diff':>8} {'status'}")
    print("-" * 60)

    total_videos = 0
    exact_matches = 0
    close_matches = 0
    mismatch_ids = []

    for video in data.get("videos", []):
        if not video.get("collected"):
            continue

        video_id = video["video_id"]
        expected = int(video["comment_count"])
        actual = actual_counts.get(video_id, 0)
        diff = actual - expected

        total_videos += 1

        if diff == 0:
            status = "ok"
            exact_matches += 1
        elif abs(diff) / max(expected, 1) < 0.05:
            status = "close"
            close_matches += 1
        else:
            status = "mismatch"
            mismatch_ids.append(video_id)

        print(f"{video_id:<15} {expected:>10} {actual:>10} {diff:>+8} {status}")

    print("-" * 60)
    print(f"total collected videos: {total_videos}")
    print(f"exact matches: {exact_matches}")
    print(f"close (<5% diff): {close_matches}")
    print(f"mismatches: {len(mismatch_ids)}")

    if mark_mismatches and mismatch_ids:
        for video in data.get("videos", []):
            if video["video_id"] in mismatch_ids:
                video["collected"] = False

        with open(f"{path}/videos_list.json", "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(
            f"\nset collected=False for {len(mismatch_ids)} videos in {path}/videos_list.json"
        )


def main():
    parser = argparse.ArgumentParser(
        description="compare comment_count from videos_list.json vs actual comments in comments.csv"
    )
    parser.add_argument(
        "--youtubers",
        nargs="+",
        help="list of space separated youtuber handles (e.g. caseoh_ mrbeast)",
    )
    parser.add_argument(
        "--mark-mismatches",
        action="store_true",
        help="set collected=False for mismatched videos in videos_list.json",
    )
    args = parser.parse_args()

    youtubers = get_youtubers(args.youtubers)
    for yt in youtubers:
        print(f"\n=== checking @{yt} ===")
        check_comments(yt, mark_mismatches=args.mark_mismatches)


if __name__ == "__main__":
    main()
