import argparse
import os

import pandas as pd

from utils import get_youtubers


def merge_datasets(path):
    datasets = [f for f in os.listdir(path) if f.endswith(".csv")]
    # datasets.append('comments_all.csv')

    print(datasets)

    dataframes = []
    all_video_ids = set()

    print("Loading datasets:")
    print("-" * 40)

    for i, dataset_name in enumerate(datasets):
        df = pd.read_csv(f"{path}{dataset_name}")
        dataframes.append(df)

        unique_videos = df["video_id"].nunique()
        video_ids = set(df["video_id"].unique())
        all_video_ids.update(video_ids)

        print(f"Dataset {i} ({dataset_name}):")
        print(f"  - Length: {len(df)}")
        print(f"  - Unique videos: {unique_videos}")
        print(f"  - Video IDs: {list(video_ids)}")
        print()

    print(f"Total unique videos across all datasets: {len(all_video_ids)}")
    print("-" * 40)

    print("Merging datasets...")
    merged_df = pd.concat(dataframes, ignore_index=True)

    del dataframes

    print(f"Total comments before deduplication: {len(merged_df)}")

    merged_df.drop_duplicates(subset=["comment_id"], inplace=True, ignore_index=True)

    print(f"Final video IDs: {list(merged_df['video_id'].unique())}")
    print(f"Total comments after deduplication: {len(merged_df)}")
    print(f"Final unique videos: {merged_df['video_id'].nunique()}")

    output_file = f"{path}comments.csv"
    merged_df.to_csv(output_file, index=False)
    print(f"Merged dataset saved to: {output_file}")

    return merged_df


def main():
    parser = argparse.ArgumentParser(
        description="Merge datasets for collected comments"
    )
    parser.add_argument(
        "--youtubers",
        nargs="+",
        help="list of space separated youtuber handles (e.g. caseoh_ mrbeast)",
    )
    args = parser.parse_args()

    youtubers = get_youtubers(args.youtubers)
    for yt in youtubers:
        print(f"\n=== Merging {yt} ===")
        merge_datasets(f"./data/@{yt}/")


if __name__ == "__main__":
    main()
