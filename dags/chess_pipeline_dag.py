"""Airflow DAG for the chess.com ETL pipeline (see pipeline.py for the original script).

Pipeline: fetch monthly game archives per player from the chess.com public API
(cached to raw_data/, one dynamically-mapped task per player-month) -> validate
records with pydantic -> build/enrich a DataFrame -> load into DuckDB.

Requires: apache-airflow, requests, pandas, pydantic, duckdb, pyarrow (parquet).

Before first run, create a pool to bound concurrent API calls (chess.com has no
documented rate limit, but hammering it with 420 parallel task instances is rude
and will get you 429s/bans -- this replaces the original script's time.sleep(0.2),
which only throttled a single sequential loop and wouldn't do anything useful once
tasks run in parallel):

    airflow pools set chess_com_api 5 "chess.com public API politeness limit"
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from pydantic import BaseModel, ValidationError

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# This file lives in dags/; raw_data, work, and chess.duckdb are mounted as
# siblings of dags/ under the Airflow home (see docker-compose.yaml volumes).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "raw_data")
WORK_DIR = os.path.join(PROJECT_ROOT, "work")
DUCKDB_PATH = os.path.join(PROJECT_ROOT, "chess.duckdb")

PLAYERS = ["hikaru", "magnuscarlsen", "gothamchess", "danielnaroditsky", "fabianocaruana"]
YEARS = range(2020, 2027)

HEADERS = {"User-Agent": "MyChessApp/1.0 (contact: your_email@example.com)"}


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


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


@dag(
    dag_id="chess_com_pipeline",
    description="Fetch, validate, and load chess.com game archives into DuckDB",
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["chess", "etl"],
)
def chess_com_pipeline():

    @task
    def get_combos() -> list[dict]:
        return [
            {"player": player, "year": year, "month": month}
            for player in PLAYERS
            for year in YEARS
            for month in range(1, 13)
        ]

    @task(retries=5, retry_delay=timedelta(seconds=30), pool="chess_com_api")
    def fetch_month(combo: dict) -> str:
        player, year, month = combo["player"], combo["year"], combo["month"]
        os.makedirs(RAW_DIR, exist_ok=True)
        cache_path = os.path.join(RAW_DIR, f"{player}_{year}_{month:02d}.json")

        if os.path.exists(cache_path):
            return cache_path

        url = f"https://api.chess.com/pub/player/{player}/games/{year}/{month:02d}"
        resp = requests.get(url, headers=HEADERS, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
        elif resp.status_code == 404:
            data = {"games": []}
        else:
            raise AirflowException(f"Failed {player} {year}-{month:02d}: {resp.status_code}")

        with open(cache_path, "w") as f:
            json.dump(data, f)
        return cache_path

    @task
    def validate_games(cache_paths: list[str]) -> str:
        os.makedirs(WORK_DIR, exist_ok=True)
        valid_path = os.path.join(WORK_DIR, "valid_games.jsonl")

        total = 0
        invalid = 0
        with open(valid_path, "w") as out:
            for cache_path in cache_paths:
                with open(cache_path) as f:
                    data = json.load(f)
                for game in data.get("games", []):
                    total += 1
                    try:
                        ChessGame(**game)
                    except ValidationError:
                        invalid += 1
                        continue
                    out.write(json.dumps(game) + "\n")

        print(f"Valid: {total - invalid} / {total} ({invalid} rejected)")
        return valid_path

    @task
    def build_dataframe(valid_games_path: str) -> dict:
        games = []
        with open(valid_games_path) as f:
            for line in f:
                games.append(json.loads(line))

        df = pd.DataFrame(games)
        df["white_result"] = pd.json_normalize(df["white"])["result"]
        df["black_result"] = pd.json_normalize(df["black"])["result"]
        df["eco"] = df["eco"] if "eco" in df.columns else None

        df["winner"] = "draw"
        df.loc[df["white_result"] == "win", "winner"] = "white"
        df.loc[df["black_result"] == "win", "winner"] = "black"

        df["opening_name"] = df["eco"].str.split("/").str[-1].str.replace("-", " ")
        df["opening_name"] = df["opening_name"].fillna("Unknown")

        opening_stats = df.groupby("eco")["winner"].value_counts(normalize=True).unstack(fill_value=0)
        opening_stats["total_games"] = df.groupby("eco").size()
        opening_stats = opening_stats.sort_values("total_games", ascending=False).reset_index()

        pivot = df.pivot_table(
            values="url",
            index="opening_name",
            columns="winner",
            aggfunc="count",
            fill_value=0,
        ).reset_index()

        os.makedirs(WORK_DIR, exist_ok=True)
        df_path = os.path.join(WORK_DIR, "games.parquet")
        stats_path = os.path.join(WORK_DIR, "opening_stats.parquet")
        pivot_path = os.path.join(WORK_DIR, "opening_pivot.parquet")

        df.to_parquet(df_path)
        opening_stats.to_parquet(stats_path)
        pivot.to_parquet(pivot_path)

        return {"games": df_path, "opening_stats": stats_path, "opening_pivot": pivot_path}

    @task
    def load_to_duckdb(paths: dict) -> None:
        import duckdb

        games = pd.read_parquet(paths["games"])
        opening_stats = pd.read_parquet(paths["opening_stats"])
        opening_pivot = pd.read_parquet(paths["opening_pivot"])

        con = duckdb.connect(DUCKDB_PATH)
        con.sql("CREATE OR REPLACE TABLE games AS SELECT * FROM games")
        con.sql("CREATE OR REPLACE TABLE opening_stats AS SELECT * FROM opening_stats")
        con.sql("CREATE OR REPLACE TABLE opening_pivot AS SELECT * FROM opening_pivot")

        print(con.sql("SELECT COUNT(*) FROM games").fetchone())
        con.close()

    combos = get_combos()
    cache_paths = fetch_month.expand(combo=combos)
    valid_path = validate_games(cache_paths)
    df_paths = build_dataframe(valid_path)
    load_to_duckdb(df_paths)


chess_com_pipeline()
