from datastore import save_seasons, save_episodes
from scraper import fetch_all_seasons, fetch_episodes_by_season

def main():
    print("Scraping seasons...")
    seasons = fetch_all_seasons()
    print(f"Got {len(seasons)} seasons.")
    save_seasons(seasons)

    print("Scraping episodes list...")
    episodes_by_season = fetch_episodes_by_season()
    print(f"Got episodes for {len(episodes_by_season)} seasons.")
    save_episodes(episodes_by_season)

    print("Done.")

if __name__ == "__main__":
    main()
