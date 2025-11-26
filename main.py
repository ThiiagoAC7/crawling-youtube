from constants import CRAWLER_PATH
from crawler.crawling import Crawling

import os
import json
import pandas as pd

def run_crawler():
    craw = Crawling()
    # craw.build_channels_list()
    # craw.build_channels_list_from_id()
    # craw.build_youtubers_videos_list_from_uploads()
    craw.build_videos_comments_df(limit=90)

def modificar_videos_json(arquivo_entrada, arquivo_saida):
        with open(arquivo_entrada, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        update_list = []
        for indice, video in enumerate(dados.get('videos', [])):
            if video['video_id'] in update_list:
                video['collected'] = False

        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)

        print(f"Arquivo '{arquivo_saida}' salvo com sucesso!")

def update_json():
    yt = "@jordanmatter"
    path = f"./data/{yt}/comments.csv"

    df = pd.read_csv(path)
    video_ids = df['video_id'].unique()

    with open(f"./data/{yt}/videos_list.json", 'r') as f:
        dados = json.load(f)

    for indice, video in enumerate(dados.get('videos', [])):
        if (video['video_id'] in video_ids) or (video['comment_count'] == "0"):
            video['collected'] = True
        else:
            video['collected'] = False

    with open(f"./data/{yt}/videos_list.json", 'w') as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)


def merge_datasets(path):
    datasets = [f for f in os.listdir(path) if f.endswith('.csv')]
    print(datasets)

    dataframes = []
    all_video_ids = set()

    print("Loading datasets:")
    print("-" * 40)

    for i, dataset_name in enumerate(datasets):
        df = pd.read_csv(f"{path}{dataset_name}")
        dataframes.append(df)

        unique_videos = df['video_id'].nunique()
        video_ids = set(df['video_id'].unique())
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

    merged_df.drop_duplicates(inplace=True, ignore_index=True)

    print(f"Total comments after deduplication: {len(merged_df)}")
    print(f"Final unique videos: {merged_df['video_id'].nunique()}")
    print(f"Final video IDs: {list(merged_df['video_id'].unique())}")

    output_file = f"{path}comments.csv"
    merged_df.to_csv(output_file, index=False)
    print(f"Merged dataset saved to: {output_file}")

    return merged_df

def verify_videos():
    yt = "@TheTrenchFamily"

    with open(f"./data/{yt}/videos_list.json", 'r') as f:
        dados = json.load(f)

    video_ids = [ ]

    i = 1
    for _, video in enumerate(dados.get('videos', [])):
        if video["video_id"] in video_ids:
            print(f"{i} -  id: {video['video_id']}, index: {video['idx']}  ")
            print(f"\t views: {video['view_count']}")
            print(f"\t comments: {video['comment_count']}")
            print(f"\t collected: {video['collected']}")
            i += 1



def main():
    
    # modificar_videos_json(f"{CRAWLER_PATH}@cristiano/videos_list.json", f"{CRAWLER_PATH}@cristiano/videos_list.json")
    # run_crawler()
    # update_json()
    path = f"{CRAWLER_PATH}@cristiano/"
    merge_datasets(path)
    # verify_videos()


if __name__ == "__main__":
    main()

