import json
from pathlib import Path

from scraper import fetch_all_seasons, fetch_episodes_by_season, enrich_episode_details
from settings import settings

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {path}")

def main():
    print("Scraping seasons...")
    seasons = fetch_all_seasons()
    print(f"Got {len(seasons)} seasons.")

    print("Scraping episodes list...")
    episodes_by_season = fetch_episodes_by_season()
    print(f"Got episodes for {len(episodes_by_season)} seasons.")

    print("Enriching per-episode details (immunity, eliminated, advantages)...")
    episodes_by_season = enrich_episode_details(episodes_by_season)

    # seasons.json
    seasons_out = {"seasons": seasons}
    write_json(DATA_DIR / "seasons.json", seasons_out)

    # episodes.json
    episodes_out = {"episodes_by_season": episodes_by_season}
    write_json(DATA_DIR / "episodes.json", episodes_out)

    # episode_details.json (compact convenience)
    details = {}
    for skey, eps in episodes_by_season.items():
        arr = []
        for e in eps:
            arr.append({
                "episode_in_season": e.get("episode_in_season"),
                "title": e.get("title"),
                "air_date": e.get("air_date"),
                "episode_page_url": e.get("episode_page_url"),
                "immunity_winners": e.get("immunity_winners", []),
                "eliminated": e.get("eliminated", []),
                "advantage_events": e.get("advantage_events", []),
            })
        details[skey] = arr
    write_json(DATA_DIR / "episode_details.json", {"episode_details_by_season": details})

if __name__ == "__main__":
    main()
