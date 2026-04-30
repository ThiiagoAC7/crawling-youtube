import argparse
import json

from constants import DEVELOPER_KEYS
from crawler.crawling import Crawling


def run_crawler(channel_ids, youtubers, api_keys, output_dir, filters):
    craw = Crawling(
        channel_ids=channel_ids,
        youtubers=youtubers,
        api_keys=api_keys,
        output_dir=output_dir,
        filters=filters,
    )
    craw.build_channels_list()
    if channel_ids:
        craw.build_channels_list_from_id()
    craw.build_youtubers_videos_list_from_uploads()
    craw.build_videos_comments_df(filter=youtubers)


def modificar_videos_json(arquivo_entrada, arquivo_saida):
    with open(arquivo_entrada, "r", encoding="utf-8") as f:
        dados = json.load(f)

    update_list = []
    for indice, video in enumerate(dados.get("videos", [])):
        if video["video_id"] in update_list:
            video["collected"] = False

    with open(arquivo_saida, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)

    print(f"Arquivo '{arquivo_saida}' salvo com sucesso!")


def main():
    parser = argparse.ArgumentParser(description="YouTube crawler")
    parser.add_argument("--config", help="path to JSON config file")
    parser.add_argument(
        "--channel-ids",
        nargs="+",
        help="list of space separated channel IDs (e.g. UC1...  UC2...)",
    )
    parser.add_argument(
        "--youtubers",
        nargs="+",
        help="list of space separated youtuber handles (e.g. caseoh_ MrBeast)",
    )
    parser.add_argument("--api-key", help="youtube data API key")
    parser.add_argument("--output-dir", help="output directory for collected data")
    parser.add_argument(
        "--start-date",
        help="filter videos published on or after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date", help="filter videos published on or before this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--min-duration", type=int, help="filter videos with duration >= N seconds"
    )
    parser.add_argument(
        "--max-duration", type=int, help="filter videos with duration <= N seconds"
    )
    args = parser.parse_args()

    config = {}
    if args.config:
        with open(args.config, "r", encoding="utf-8") as f:
            config = json.load(f)

    api_keys = DEVELOPER_KEYS
    if args.api_key:
        api_keys = [args.api_key]
    if config.get("api_keys"):
        api_keys = config["api_keys"]

    if not api_keys:
        raise ValueError("no api keys provided. use --api-key, --config, or .env")

    _config_filters = config.get("filters", {})
    filters = {
        k: v
        for k, v in {
            "start_date": args.start_date or _config_filters.get("start_date"),
            "end_date": args.end_date or _config_filters.get("end_date"),
            "min_duration": args.min_duration or _config_filters.get("min_duration"),
            "max_duration": args.max_duration or _config_filters.get("max_duration"),
        }.items()
        if v is not None
    }

    run_crawler(
        channel_ids=config.get("channel_ids", args.channel_ids),
        youtubers=config.get("youtubers", args.youtubers),
        api_keys=api_keys,
        output_dir=config.get("output_dir", args.output_dir),
        filters=filters if filters else None,
    )


if __name__ == "__main__":
    main()
