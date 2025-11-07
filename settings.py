from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    # Fandom Survivor wiki base
    WIKI_BASE: str = "https://survivor.fandom.com"
    API_ENDPOINT: str = WIKI_BASE + "/api.php"

    # A descriptive user agent is polite
    USER_AGENT: str = "SurvivorAPI-Bot/1.0 (+https://example.com/contact) TrishaPersonalProject"

    # How long to sleep between API calls (seconds)
    REQUEST_DELAY: float = 0.75

    # Only scrape Survivor (U.S.) seasons
    # We'll build the list of pages by following season numbers 1..current and possible numeric titles
    # and cross-check that the page has the expected "Season Information" block.
    MIN_SEASON: int = 1
    # You can raise this if new seasons appear later; we'll also try numeric titles like 'Survivor 49'
    MAX_SEASON_GUESS: int = 60  # generous upper bound; scraper checks existence

    DATA_DIR: str = "data"  # folder where JSON is stored

settings = Settings()
