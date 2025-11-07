from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from datastore import load_seasons, load_episodes

api = FastAPI(title="Survivor API (Unofficial)", version="1.0.0")

@api.get("/health")
def health():
    return {"ok": True}

@api.get("/seasons")
def list_seasons():
    data = load_seasons()
    return data

@api.get("/seasons/{season_number}")
def get_season(season_number: int):
    data = load_seasons()
    for s in data.get("seasons", []):
        if s.get("season_number") == season_number:
            return s
    raise HTTPException(status_code=404, detail="Season not found")

@api.get("/episodes")
def get_episodes(season: Optional[int] = Query(None, description="Season number (e.g., 48)")):
    data = load_episodes()
    if season is None:
        return data
    key = str(season)
    eps = data.get("episodes_by_season", {}).get(key)
    if eps is None:
        raise HTTPException(status_code=404, detail="No episodes for that season")
    return {"season": season, "episodes": eps}

# (Room to add: /castaways, /votes etc. in future)
