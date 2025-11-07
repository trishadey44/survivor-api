import re
import time
from typing import Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from settings import settings
from datastore import load_seasons

S = requests.Session()
S.headers.update({"User-Agent": settings.USER_AGENT})

API = settings.API_ENDPOINT

def _sleep():
    time.sleep(settings.REQUEST_DELAY)

def _mediawiki_parse_html(title: str) -> Tuple[str, Optional[str]]:
    """
    Use MediaWiki Action API to fetch parsed HTML for a page title.
    Returns (html, canonical_url or None).
    """
    params = {
        "action": "parse",
        "page": title,
        "prop": "text|displaytitle|externallinks|links|sections",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    r = S.get(API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        return "", None
    html = data["parse"]["text"]
    canonical = settings.WIKI_BASE + "/wiki/" + requests.utils.quote(data["parse"]["title"].replace(" ", "_"))
    return html, canonical

def _season_title_variants(n: int) -> List[str]:
    """
    Possible wiki page titles for a season.
    Old seasons have names like 'Survivor: Borneo'
    New-era seasons use numeric titles 'Survivor 44', etc.
    We'll try both the known canon names and numeric form.
    """
    numeric = f"Survivor {n}"
    # A small seed of well-known early season names to speed up lookups if needed
    early_map = {
        1: "Survivor: Borneo",
        2: "Survivor: The Australian Outback",
        3: "Survivor: Africa",
        4: "Survivor: Marquesas",
        5: "Survivor: Thailand",
        6: "Survivor: The Amazon",
        7: "Survivor: Pearl Islands",
        8: "Survivor: All-Stars",
        9: "Survivor: Vanuatu",
        10: "Survivor: Palau",
        11: "Survivor: Guatemala",
        12: "Survivor: Panama",
        13: "Survivor: Cook Islands",
        14: "Survivor: Fiji",
        15: "Survivor: China",
        16: "Survivor: Micronesia",
        17: "Survivor: Gabon",
        18: "Survivor: Tocantins",
        19: "Survivor: Samoa",
        20: "Survivor: Heroes vs. Villains",
        21: "Survivor: Nicaragua",
        22: "Survivor: Redemption Island",
        23: "Survivor: South Pacific",
        24: "Survivor: One World",
        25: "Survivor: Philippines",
        26: "Survivor: Caramoan",
        27: "Survivor: Blood vs. Water",
        28: "Survivor: Cagayan",
        29: "Survivor: San Juan del Sur",
        30: "Survivor: Worlds Apart",
        31: "Survivor: Cambodia",
        32: "Survivor: Kaôh Rōng",
        33: "Survivor: Millennials vs. Gen X",
        34: "Survivor: Game Changers",
        35: "Survivor: Heroes vs. Healers vs. Hustlers",
        36: "Survivor: Ghost Island",
        37: "Survivor: David vs. Goliath",
        38: "Survivor: Edge of Extinction",
        39: "Survivor: Island of the Idols",
        40: "Survivor: Winners at War",
    }
    variants = []
    if n in early_map:
        variants.append(early_map[n])
    variants.append(numeric)
    return variants

def _extract_text(el) -> str:
    return " ".join(el.get_text(" ", strip=True).split())

def _parse_season_info_block(soup: BeautifulSoup) -> Dict[str, str]:
    """
    The season page contains a 'Season Information' section with h3 headings and values.
    We'll find the h2 with text 'Season Information', then walk h3s until the next h2.
    """
    out: Dict[str, str] = {}
    # find the H2
    h2 = None
    for tag in soup.find_all(["h2", "h3"]):
        if tag.name == "h2" and "Season Information" in _extract_text(tag):
            h2 = tag
            break
    if not h2:
        return out
    # iterate siblings until next h2
    for sib in h2.find_all_next():
        if sib.name == "h2" and sib is not h2:
            break
        if sib.name == "h3":
            key = _extract_text(sib)
            # value is usually in the next sibling(s)
            val_parts = []
            nxt = sib.find_next_sibling()
            # gather until next h3/h2
            while nxt and nxt.name not in ["h3", "h2"]:
                val_parts.append(_extract_text(nxt))
                nxt = nxt.find_next_sibling()
            val = " ".join([p for p in val_parts if p])
            out[key] = val
    return out

def _parse_viewership_millions(info_map: Dict[str, str]) -> Optional[float]:
    val = info_map.get("Viewership (in Millions)")
    if not val:
        return None
    # Example: "28.30[ 1 ]" or "5.54"
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", val.replace(",", ""))
    return float(m.group(1)) if m else None

def _parse_tribes(info_map: Dict[str, str]) -> List[str]:
    txt = info_map.get("Tribes", "")
    # Tribes often look like: "Pagong Tagi Rattana" or bullets; we split by separators.
    return [t.strip("•-– ").strip() for t in re.split(r"[,\|\u00B7]| {2,}| - ", txt) if t.strip()]

def _parse_dates_range(s: str) -> Tuple[Optional[str], Optional[str]]:
    # "March 13, 2000 - April 20, 2000" or "September 24, 2025 -"
    if not s:
        return None, None
    parts = [p.strip() for p in s.split("-")]
    start = parts[0] if parts else None
    end = parts[1] if len(parts) > 1 and parts[1] else None
    return (start or None, end or None)

def fetch_one_season(n: int) -> Optional[Dict]:
    """
    Try known titles for this season; if a page exists and contains 'Season Information',
    return a structured dict.
    """
    for title in _season_title_variants(n):
        _sleep()
        html, url = _mediawiki_parse_html(title)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        info = _parse_season_info_block(soup)
        # must have Season No. or No. of Episodes to count as a real season page
        if not info:
            continue

        name = soup.find("h1")
        display_name = _extract_text(name) if name else title

        season_no_str = info.get("Season No.") or str(n)
        try:
            season_no = int(re.search(r"\d+", season_no_str).group(0))
        except Exception:
            season_no = n

        filming_loc = info.get("Filming Location") or ""
        filming_start, filming_end = _parse_dates_range(info.get("Filming Dates", ""))
        run_start, run_end = _parse_dates_range(info.get("Season Run", ""))

        def _to_int_field(k: str) -> Optional[int]:
            v = info.get(k)
            if not v:
                return None
            m = re.search(r"\d+", v.replace(",", ""))
            return int(m.group(0)) if m else None

        num_eps = _to_int_field("No. of Episodes")
        num_days = _to_int_field("No. of Days")
        num_cast = _to_int_field("No. of Castaways")

        winner_txt = info.get("Winner") or ""

        tribes = _parse_tribes(info)
        view_millions = _parse_viewership_millions(info)

        return {
            "season_number": season_no,
            "title": display_name,
            "location": filming_loc,
            "filming_dates": {
                "start": filming_start,
                "end": filming_end
            },
            "airing_dates": {
                "start": run_start,
                "end": run_end
            },
            "num_episodes": num_eps,
            "num_days": num_days,
            "num_castaways": num_cast,
            "winner": winner_txt,
            "tribes": tribes,
            "viewership_millions": view_millions,
            "source_url": url,
        }
    return None

def discover_current_max_season() -> int:
    """
    We can peek the main Survivor (U.S.) page or Wikipedia to learn current count,
    but to keep this self-contained, we’ll just probe pages until one fails for a while.
    """
    hi = 0
    for n in range(settings.MIN_SEASON, settings.MAX_SEASON_GUESS + 1):
        s = fetch_one_season(n)
        if s is None:
            # allow up to 3 consecutive misses before stopping (future placeholder seasons may exist)
            miss = 0
            # try numeric-only title explicitly once more to be safe
            _sleep()
            html, _ = _mediawiki_parse_html(f"Survivor {n}")
            if html:
                hi = n
                continue
            break
        else:
            hi = n
    return hi

def fetch_all_seasons() -> List[Dict]:
    # Try up to a reasonable maximum, stopping when pages no longer resolve.
    seasons: List[Dict] = []
    max_seen = 0
    for n in range(settings.MIN_SEASON, settings.MAX_SEASON_GUESS + 1):
        data = fetch_one_season(n)
        if data:
            seasons.append(data)
            max_seen = n
        else:
            # stop if we passed a long gap (assume end)
            if n > max_seen + 2:
                break
    # sort by season_number just in case
    seasons.sort(key=lambda x: x["season_number"])
    return seasons

# ---- Episodes (basic) ----

def fetch_episodes_by_season() -> Dict[str, List[Dict]]:
    """
    Parse the big episodes table on:
      https://survivor.fandom.com/wiki/List_of_Survivor_(U.S.)_episodes
    and group rows by season number.

    We map the table's "Season" cell (e.g., 'Borneo', 'The Australian Outback', 'Survivor 41')
    to an integer season_number using the seasons we've already scraped into data/seasons.json.
    """
    # 1) Build a lookup from season title variants -> season_number
    seasons_data = load_seasons()
    title_to_num: Dict[str, int] = {}
    for s in seasons_data.get("seasons", []):
        num = s.get("season_number")
        title = (s.get("title") or "").strip()
        if num and title:
            # Title is usually "Survivor: Borneo" or "Survivor 48" etc.
            title_to_num[title.lower()] = num

            # Also store a few helpful aliases:
            # - drop leading "Survivor:" prefix if present
            alias = title
            if ":" in alias:
                alias = alias.split(":", 1)[1].strip()
                title_to_num[alias.lower()] = num
            # - if it's "Survivor 48", store "48" and "Survivor48" just in case
            m = re.search(r"\b(\d{1,3})\b", title)
            if m:
                title_to_num[m.group(1).lower()] = num
                title_to_num[f"survivor {m.group(1)}"] = num

    # 2) Fetch parsed HTML for the list page via MediaWiki API
    _sleep()
    html, page_url = _mediawiki_parse_html("List of Survivor (U.S.) episodes")
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")

    # 3) Find the main episodes table: look for a table whose header has "Season" and "Episode Title"
    target_table = None
    for tbl in soup.find_all("table"):
        headers = [h.get_text(" ", strip=True).lower() for h in tbl.find_all("th")]
        if headers and ("season" in " ".join(headers)) and ("episode title" in " ".join(headers)):
            target_table = tbl
            break

    if target_table is None:
        return {}

    # 4) Identify the column indices
    header_cells = [h.get_text(" ", strip=True).lower() for h in target_table.find("tr").find_all(["th", "td"])]
    def col_idx(name_opts: List[str]) -> int:
        for i, h in enumerate(header_cells):
            for opt in name_opts:
                if opt in h:
                    return i
        return -1

    season_col = col_idx(["season"])
    epno_col   = col_idx(["episode no."])   # per-season episode number
    overall_col= col_idx(["overall"])       # overall episode number
    air_col    = col_idx(["air date"])
    title_col  = col_idx(["episode title"])
    type_col   = col_idx(["episode type"])

    # 5) Walk rows and group by season_number
    episodes_by_season: Dict[str, List[Dict]] = {}
    current_season_num: Optional[int] = None

    for tr in target_table.find_all("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        # season cell sometimes repeats only on the first row for that season
        season_txt = ""
        if season_col != -1 and season_col < len(cells):
            season_txt = _extract_text(cells[season_col]).strip()

        # If we see a season value in this row, update current season mapping
        if season_txt:
            # Prefer link text if present (cleaner than extra whitespace)
            link = cells[season_col].find("a")
            if link:
                season_txt = _extract_text(link).strip() or season_txt

            key = season_txt.lower()

            # Try direct match first
            num = title_to_num.get(key)

            # Heuristics for aliases:
            if num is None:
                # If it reads "Survivor 49" etc, normalize spacing
                if re.match(r"^survivor\s+\d{1,3}$", key):
                    num = title_to_num.get(key)
                # If it reads just the nickname ("Borneo", "Africa", etc.)
                if num is None:
                    # Try prepending "Survivor:" and looking up
                    num = title_to_num.get(f"survivor: {key}")
                # If it includes parentheses like "(U.S.)", strip them
                if num is None:
                    key2 = re.sub(r"\s*\(.*?\)\s*", "", key).strip()
                    num = title_to_num.get(key2) or title_to_num.get(f"survivor: {key2}")

                # Finally, if the cell ALSO contains a number, use that
                if num is None:
                    m = re.search(r"\b(\d{1,3})\b", season_txt)
                    if m:
                        num = title_to_num.get(m.group(1).lower())

            current_season_num = num

        # If we still don't know the season, skip this row (can't group it)
        if current_season_num is None:
            continue

        def grab(col: int) -> Optional[str]:
            if col == -1 or col >= len(cells):
                return None
            return _extract_text(cells[col]) or None

        ep_season_no = grab(epno_col)
        ep_overall   = grab(overall_col)
        air_date     = grab(air_col)
        ep_type      = grab(type_col)

        title_txt = None
        if title_col != -1 and title_col < len(cells):
            # prefer link text → also capture episode page URL if present
            link = cells[title_col].find("a")
            if link:
                title_txt = _extract_text(link)
                # build episode page URL if available
                href = link.get("href")
                if href and href.startswith("/wiki/"):
                    ep_url = settings.WIKI_BASE + href
                else:
                    ep_url = page_url
            else:
                title_txt = _extract_text(cells[title_col])
                ep_url = page_url
        else:
            ep_url = page_url

        # Clean numeric fields where possible
        def to_int(maybe: Optional[str]) -> Optional[int]:
            if not maybe:
                return None
            m = re.search(r"\d+", maybe.replace(",", ""))
            return int(m.group(0)) if m else None

        rec = {
            "episode_in_season": to_int(ep_season_no),
            "episode_overall": to_int(ep_overall),
            "title": title_txt,
            "air_date": air_date,
            "type": ep_type,
            "source_url": ep_url
        }

        key = str(current_season_num)
        episodes_by_season.setdefault(key, []).append(rec)

    return episodes_by_season
