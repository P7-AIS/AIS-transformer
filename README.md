# AIS-transformer

## Prerequisites
1. Install PostgreSQL
   1. Win: You need to add the PostgreSQL bin folder (which contains libpq.dll) to your system's PATH
2. Create a virtual environment: `python -m venv .venv`
3. Activate environment
   1. Win: `.\.venv\Scripts\Activate.ps1`
   2. Mac: `source .venv/bin/activate`
4. Install requirements: `pip install -r requirements.txt` 

## Run
1. Activate environment
   1. Win: `.\.venv\Scripts\Activate.ps1`
   2. Mac: `source .venv/bin/activate`
2. Run script: `python ./src/main.py -f {path/to/zipfile}.zip`