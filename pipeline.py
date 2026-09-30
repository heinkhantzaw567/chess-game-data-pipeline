players = ["hikaru", "magnuscarlsen", "gothamchess", "danielnaroditsky", "fabianocaruana"]
years = range(2020, 2027)

import requests, time, json, os

headers = {"User-Agent": "MyChessApp/1.0 (contact: your_email@example.com)"}
cache_dir = "raw_data"
os.makedirs(cache_dir, exist_ok=True)

def fetch_month(player, year, month):
    cache_path = f"{cache_dir}/{player}_{year}_{month:02d}.json"
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            return json.load(f)

    url = f"https://api.chess.com/pub/player/{player}/games/{year}/{month:02d}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        with open(cache_path, "w") as f:
            json.dump(data, f)
        return data
    else:
        print(f"Failed {player} {year}-{month:02d}: {resp.status_code}")
        return {"games": []}



all_games = []
for player in players:
    for year in years:
        for month in range(1, 13):
            data = fetch_month(player, year, month)
            all_games.extend(data.get("games", []))
            time.sleep(0.2)  # stay polite to the API

print(f"Total games: {len(all_games)}")


import pandas as pd

df = pd.DataFrame(all_games)
white_ratings = pd.json_normalize(df['white'])['rating']
black_ratings = pd.json_normalize(df['black'])['rating']

import time
start = time.time()
diffs_vectorized = (white_ratings - black_ratings).abs()
print(f"Vectorized: {time.time()-start:.4f}s")

start = time.time()
diffs_loop = [abs(w - b) for w, b in zip(white_ratings, black_ratings)]
print(f"Loop: {time.time()-start:.4f}s")


df['white_result'] = pd.json_normalize(df['white'])['result']
df['black_result'] = pd.json_normalize(df['black'])['result']
df['eco'] = df['eco'] if 'eco' in df.columns else None

df['winner'] = 'draw'
df.loc[df['white_result'] == 'win', 'winner'] = 'white'
df.loc[df['black_result'] == 'win', 'winner'] = 'black'

opening_stats = df.groupby('eco')['winner'].value_counts(normalize=True).unstack(fill_value=0)
opening_stats['total_games'] = df.groupby('eco').size()
opening_stats = opening_stats.sort_values('total_games', ascending=False)



df['opening_name'] = df['eco'].str.split('/').str[-1].str.replace('-', ' ')
df['opening_name'] = df['opening_name'].fillna('Unknown')


pivot = df.pivot_table(
    values='url',      # different col than index/columns
    index='opening_name',
    columns='winner',
    aggfunc='count',
    fill_value=0
)




from pydantic import BaseModel, ValidationError
from typing import Optional

class PlayerResult(BaseModel):
    username: str
    rating: int
    result: str


class ChessGame(BaseModel):
    url: str
    eco: Optional[str] = None
    time_class: str
    white: PlayerResult
    black: PlayerResult


valid_games = []
invalid_count = 0
for game in all_games:
    try:
        ChessGame(**game)
        valid_games.append(game)
    except ValidationError:
        invalid_count += 1

print(f"Valid: {len(valid_games)} / {len(all_games)} ({invalid_count} rejected)")
all_games = valid_games  # continue the pipeline with only clean records



import duckdb

con = duckdb.connect("chess.duckdb")  # or ":memory:" if you don't want it persisted
con.sql("CREATE OR REPLACE TABLE games AS SELECT * FROM df") 

# sanity check
print(con.sql("SELECT COUNT(*) FROM games").fetchone())