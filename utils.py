import os


def get_youtubers(youtubers_filter=None):
    data_dir = "./data"
    all_youtubers = [
        d[1:] # no @
        for d in os.listdir(data_dir)
        if os.path.isdir(os.path.join(data_dir, d)) and d.startswith("@")
    ]
    all_youtubers.sort()

    if youtubers_filter:
        return [yt for yt in all_youtubers if yt in youtubers_filter]

    return all_youtubers
