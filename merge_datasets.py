import argparse
import os

import polars as pl

from utils import get_youtubers


def merge_datasets(path, verbose=False):
    datasets = [f for f in os.listdir(path) if f.endswith(".csv") and f != "comments.csv"]

    print(datasets)

    lazy_frames = []
    all_video_ids = set()

    print("Loading datasets:")
    print("-" * 40)

    for i, dataset_name in enumerate(datasets):
        lf = pl.scan_csv(f"{path}{dataset_name}", schema_overrides={"parent_comment_id": pl.String})

        if verbose:
            df = lf.collect()
            print(f"Dataset {i} ({dataset_name}):")
            print(f"  - Length: {len(df)}")
            unique_videos = df["video_id"].n_unique()
            video_ids = set(df["video_id"].unique().to_list())
            all_video_ids.update(video_ids)
            print(f"  - Unique videos: {unique_videos}")
            print(f"  - Video IDs: {list(video_ids)}")
            print()
            lazy_frames.append(df.lazy())
        else:
            lazy_frames.append(lf)

    if verbose:
        print(f"Total unique videos across all datasets: {len(all_video_ids)}")
    print("-" * 40)

    print("Merging datasets...")
    merged_lf = pl.concat(lazy_frames)
    merged_lf = merged_lf.unique(subset=["comment_id"])

    print("Collecting...")
    merged_df = merged_lf.collect(engine="streaming")

    print(f"Total comments after deduplication: {len(merged_df)}")
    print(f"Final unique videos: {merged_df['video_id'].n_unique()}")

    output_file = f"{path}comments.parquet"
    merged_lf.sink_parquet(output_file)
    print(f"Merged dataset saved to: {output_file}")

    # return merged_df


def main():
    parser = argparse.ArgumentParser(
        description="Merge datasets for collected comments"
    )
    parser.add_argument(
        "--youtubers",
        nargs="+",
        help="list of space separated youtuber handles (e.g. caseoh_ mrbeast)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print per-file stats (loads each file eagerly, uses more RAM)",
    )
    args = parser.parse_args()

    youtubers = get_youtubers(args.youtubers)
    for yt in youtubers:
        print(f"\n=== Merging {yt} ===")
        merge_datasets(f"./data/@{yt}/", verbose=args.verbose)


if __name__ == "__main__":
    main()
