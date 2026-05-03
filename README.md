# Data Processing and REST API for Game Events

## Overview
This project processes raw game event data from Golf Rival and exposes it through a REST API. 
The pipeline reads events from a JSONL file, cleans and validates them, stores them in a SQLite 
database, and serves two API endpoints for querying player and map statistics.

## Project Structure
```
data_processing/
├── data/
│   ├── events.jsonl        # raw game events
│   └── maps.jsonl          # map info
├── processing.py           # data cleaning and transformation pipeline
├── database.py             # database setup and data insertion
├── main.py                 # FastAPI REST API
├── chart.py                # bonus: match count line chart per map
└── README.md
```

## Setup

### Requirements
- Python 3.8+
- pip

### Install dependencies
```bash
pip install fastapi uvicorn matplotlib
```

## How to run

### Step 1 — Process and load data
Run this once to clean the data and populate the database:
```bash
python processing.py
```

### Step 2 — Start the API
```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`.
Interactive docs at `http://localhost:8000/docs`.

## API Endpoints

### GET /user-stats
Returns player statistics ordered by total playtime descending.

**Optional query parameters:**
- `countries` — filter by country code (can be repeated for multiple)
- `OSs` — filter by session OS: `iOS` or `Android` (can be repeated)

**Example:**
GET http://localhost:8000/user-stats?countries=SRB&countries=HRV&OSs=Android

**Response fields:**
- `username` — player username
- `country` — registration country
- `fav_map` — map with highest win ratio
- `fav_map_win_ratio` — win ratio on favorite map
- `total_playtime` — total time spent in game in seconds
- `total_win_ratio` — total wins divided by total matches
- `avg_matches_per_session` — average number of matches played per session
- `registration_date` — date of registration (YYYY-MM-DD)

---

### GET /map-stats/{map_name}
Returns daily statistics for a specific map, ordered by date descending.

**Optional query parameters:**
- `date_from` — start date in YYYY-MM-DD format
- `date_to` — end date in YYYY-MM-DD format

**Example:**
GET http://localhost:8000/map-stats/Desert?date_from=2026-04-05&date_to=2026-04-07

**Response fields:**
- `date` — the date the stats are for
- `avg_playtime` — average match duration in seconds on that date
- `best_player_username` — player with highest cumulative win ratio on this map up to and including that date
- `match_cnt` — number of matches played on that date

## Design decisions and assumptions

- **Session tracking** is done without using the `state` field — sessions are reconstructed by grouping pings per user, sorting by timestamp, and splitting whenever the gap between two consecutive pings exceeds 120 seconds.
- **Partial matches** are considered valid if there is at least one `match_start` and one `match_finish` between two players. If a second `match_start` appears before the first match finishes, the first match is discarded as invalid.
- **Users without a registration event** are excluded from all statistics — their sessions and matches are not inserted into the database.
- **Zero duration sessions** (only one valid ping) are kept — they represent very short gameplay sessions and contribute 0 to total playtime.
- **Duplicate events** are resolved by keeping the event with the earliest timestamp.

## Bonus: Match Count Chart

To generate a line chart showing match count per map over the last 7 days, run:

```bash
python chart.py
```

This will display the chart and save it as `match_chart.png` in the project folder.