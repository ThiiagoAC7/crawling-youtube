# YOUTUBE COMMENTS CRAWLER

TODO: more detailed description

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
