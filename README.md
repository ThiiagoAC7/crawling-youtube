# YOUTUBE COMMENTS CRAWLER

craws comments from preconfigured youtube channels.

## USAGE

pass either --channel-ids (list of channel IDS) or --youtubers (list of youtuber @ (as it appears on
their channel's url). Default approach is to configure at `constants.py`, and set API keys at a
`.env`. Or, create a .json config and pass through --config:

```json
{
  "channel_ids": ["UC_...", "UC2_..."],
  "youtubers": ["@caseoh_", "@mrbeast"],
  "api_keys": ["KEY1", "KEY2", "KEY3"],
  "output_dir": "./data",
  "filters": {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "min_duration": 1,
    "max_duration": 9999999
  }
}
```

optional filters for video collection:

- `--start-date`: only collect videos published on or after this date
  (YYYY-MM-DD). defaults to 2005-01-01 (before youtube existed)
- `--end-date`: only collect videos published on or before this date
  (YYYY-MM-DD). defaults to now
- `--min-duration`: only collect videos with duration >= N seconds
- `--max-duration`: only collect videos with duration <= N seconds

- note: passing args overrides the configs at `constants.py`. a safe approach is to save multiple
  api keys at `.env` and call them from `constants.py`, keeping the keys safe and the rest more
  dynamic

## SETUP

use a virtual environment and pip:

```bash
# create and activate venv
python -m venv .venv
source .venv/bin/activate

# install dependencies
pip install -r requirements.txt
```

### with pixi

to setup with pixi, just install and run:

```bash
pixi install # installs the environment

pixi run main
```

## TODO

- [x] api key rotation handler
- [ ] better error handling for when all api keys are used -> do this outside crawling.py
- [ ] arg to filter by youtubers when collecting, usefull to check api limits
- [ ] better name when saving the final .csv
- [ ] possible data preprocessing -> dont collect channel @ and mask @handles on comments -> @user
