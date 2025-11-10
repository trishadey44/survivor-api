import re
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from settings import settings

# ------------------------------------------------------------
# HTTP session + helpers (with retry)
# ------------------------------------------------------------

S = requests.Session()
S.headers.update({"User-Agent": settings.USER_AGENT})
API = settings.API_ENDPOINT

def _sleep():
    time.sleep(settings.REQUEST_DELAY)

def _mediawiki_parse_html(title: str) -> Tuple[str, Optional[str]]:
    """
    Use MediaWiki Action API to fetch parsed HTML for a page title.
    Returns (html, canonical_url or None).
    Retries politely on transient errors.
    """
    params = {
        "action": "parse",
        "page": title,
        "prop": "text|displaytitle|externallinks|links|sections",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    last_exc = None
    for attempt in range(3):
        try:
            r = S.get(API, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                return "", None
            html = data["parse"]["text"]
            canonical = settings.WIKI_BASE + "/wiki/" + requests.utils.quote(
                data["parse"]["title"].replace(" ", "_")
            )
            return html, canonical
        except Exception as e:
            last_exc = e
            time.sleep(settings.REQUEST_DELAY * (attempt + 1))
    # final failure
    return "", None

def _extract_text(el) -> str:
    if not el:
        return ""
    return " ".join(el.get_text(" ", strip=True).split())

# ------------------------------------------------------------
# Validation / cleaning helpers
# ------------------------------------------------------------

DATE_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}$"
)

BAD_AIRDATE_TOKENS = {
    "—", "-", "— —", "tbd", "tbp", "unknown", "n/a", "na", "tba", "tbc"
}

def _is_valid_air_date(s: Optional[str]) -> bool:
    if not s:
        return False
    s = s.strip()
    if s.lower() in BAD_AIRDATE_TOKENS:
        return False
    return bool(DATE_RE.match(s))

def _clean_air_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = " ".join(str(s).split())
    s = re.sub(r"^(air\s*date\s*:)\s*", "", s, flags=re.I)
    s = re.sub(r"\[[^\]]+\]", "", s).strip()
    if _is_valid_air_date(s):
        return s
    return None

def _looks_like_notes_blob(s: str) -> bool:
    s = (s or "").lower().strip()
    return (
        s.startswith("notes:") or s.startswith("note:") or
        "combined reward and immunity" in s or
        bool(re.search(r"\^\d", s))
    )

def _looks_like_name_garbage(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    if s.startswith("/") or " / " in s:
        return True
    # single capitalized token (very likely a stray name like "Jessica")
    return bool(re.match(r"^[A-Z][a-zA-Z'-]{1,}$", s))

def _cell_text(cell) -> str:
    for br in cell.find_all("br"):
        br.replace_with(" / ")
    return _extract_text(cell)

def _parse_span(value, default=1, hard_cap=50) -> int:
    if value is None or value == "":
        return default
    m = re.search(r"\d+", str(value))
    if not m:
        return default
    n = int(m.group(0))
    return max(1, min(n, hard_cap))

def _normalize_table_rows(table) -> List[List[str]]:
    """
    Build a rectangular matrix from a wikitable with colspan/rowspan support
    and ignore sortbottom/separator rows. Hardened against weird colspans.
    """
    rows = table.find_all("tr")
    if not rows:
        return []

    header_tr = None
    for tr in rows:
        if tr.find_all("th"):
            header_tr = tr
            break
    if not header_tr:
        return []

    # count columns from header with colspans
    num_cols = 0
    for th in header_tr.find_all("th"):
        num_cols += _parse_span(th.get("colspan"), default=1)

    # header row (expanded)
    matrix: List[List[str]] = []
    header_row: List[str] = []
    for th in header_tr.find_all("th"):
        text = _cell_text(th)
        cspan = _parse_span(th.get("colspan"), default=1)
        for _ in range(cspan):
            header_row.append(text)
    if len(header_row) < num_cols:
        header_row.extend([""] * (num_cols - len(header_row)))
    elif len(header_row) > num_cols:
        header_row = header_row[:num_cols]
    matrix.append(header_row)

    pending: List[Optional[Tuple[str, int]]] = [None] * num_cols

    def build_row_from_tr(tr) -> List[str]:
        row: List[str] = []
        cells = tr.find_all(["td", "th"])
        it = iter(cells)
        col = 0
        while col < num_cols:
            if pending[col]:
                val, rem = pending[col]
                row.append(val)
                rem -= 1
                pending[col] = (val, rem) if rem > 0 else None
                col += 1
                continue
            try:
                cell = next(it)
            except StopIteration:
                row.extend([""] * (num_cols - len(row)))
                break
            text = _cell_text(cell)
            rspan = _parse_span(cell.get("rowspan"), default=1)
            cspan = _parse_span(cell.get("colspan"), default=1)
            for _ in range(min(cspan, num_cols - col)):
                row.append(text)
                if rspan > 1:
                    pending[col] = (text, rspan - 1)
                col += 1
        if len(row) < num_cols:
            row.extend([""] * (num_cols - len(row)))
        elif len(row) > num_cols:
            row = row[:num_cols]
        return row

    start_idx = rows.index(header_tr) + 1
    for tr in rows[start_idx:]:
        if not tr.find_all(["td", "th"]):
            continue
        if 'class' in tr.attrs and any('sortbottom' in c for c in tr['class']):
            continue
        matrix.append(build_row_from_tr(tr))

    return matrix

# ------------------------------------------------------------
# Seasons scraping
# ------------------------------------------------------------

def _season_title_variants(n: int) -> List[str]:
    numeric = f"Survivor {n}"
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

def _parse_season_info_block(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}
    h2 = None
    for tag in soup.find_all(["h2", "h3"]):
        if tag.name == "h2" and "Season Information" in _extract_text(tag):
            h2 = tag
            break
    if not h2:
        return out
    for sib in h2.find_all_next():
        if sib.name == "h2" and sib is not h2:
            break
        if sib.name == "h3":
            key = _extract_text(sib)
            val_parts = []
            nxt = sib.find_next_sibling()
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
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", val.replace(",", ""))
    return float(m.group(1)) if m else None

def _parse_tribes(info_map: Dict[str, str]) -> List[str]:
    txt = info_map.get("Tribes", "")
    return [t.strip("•-– ").strip() for t in re.split(r"[,\|\u00B7]| {2,}| - ", txt) if t.strip()]

def _parse_dates_range(s: str) -> Tuple[Optional[str], Optional[str]]:
    if not s:
        return None, None
    parts = [p.strip() for p in s.split("-")]
    start = parts[0] if parts else None
    end = parts[1] if len(parts) > 1 and parts[1] else None
    return (start or None, end or None)

def fetch_one_season(n: int) -> Optional[Dict]:
    for title in _season_title_variants(n):
        _sleep()
        html, url = _mediawiki_parse_html(title)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        info = _parse_season_info_block(soup)
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
            "filming_dates": {"start": filming_start, "end": filming_end},
            "airing_dates": {"start": run_start, "end": run_end},
            "num_episodes": num_eps,
            "num_days": num_days,
            "num_castaways": num_cast,
            "winner": winner_txt,
            "tribes": tribes,
            "viewership_millions": view_millions,
            "source_url": url,
        }
    return None

def fetch_all_seasons() -> List[Dict]:
    """
    Try to gather as many season info pages as possible without guessing when to stop.
    We do NOT early-break; we sweep the whole configured range and keep what succeeds.
    """
    seasons: List[Dict] = []
    for n in range(settings.MIN_SEASON, settings.MAX_SEASON_GUESS + 1):
        data = fetch_one_season(n)
        if data:
            seasons.append(data)
    seasons.sort(key=lambda x: x["season_number"])
    return seasons

# ------------------------------------------------------------
# Episodes scraping (master table + per-season fallback + list parsing)
# ------------------------------------------------------------

def _table_has_headers(th_texts: List[str], required: List[str]) -> bool:
    lower = [t.lower() for t in th_texts]
    return all(any(req.lower() in h for h in lower) for req in required)

def _clean_quoted(s: str) -> str:
    return (s or "").strip().strip('“”"\' ').strip()

def _to_int(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group(0)) if m else None

def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace(",", "")
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", s)
    return float(m.group(1)) if m else None

def _season_number_from_cell(text: str) -> Optional[int]:
    t = (text or "").strip()
    m = re.search(r"(\d+)", t)
    if m:
        return int(m.group(1))
    name_map = {
        "Borneo": 1, "The Australian Outback": 2, "Africa": 3, "Marquesas": 4, "Thailand": 5,
        "The Amazon": 6, "Pearl Islands": 7, "All-Stars": 8, "Vanuatu": 9, "Palau": 10,
        "Guatemala": 11, "Panama": 12, "Cook Islands": 13, "Fiji": 14, "China": 15,
        "Micronesia": 16, "Gabon": 17, "Tocantins": 18, "Samoa": 19,
        "Heroes vs. Villains": 20, "Nicaragua": 21, "Redemption Island": 22,
        "South Pacific": 23, "One World": 24, "Philippines": 25, "Caramoan": 26,
        "Blood vs. Water": 27, "Cagayan": 28, "San Juan del Sur": 29, "Worlds Apart": 30,
        "Cambodia": 31, "Kaôh Rōng": 32, "Millennials vs. Gen X": 33, "Game Changers": 34,
        "Heroes vs. Healers vs. Hustlers": 35, "Ghost Island": 36, "David vs. Goliath": 37,
        "Edge of Extinction": 38, "Island of the Idols": 39, "Winners at War": 40,
    }
    return name_map.get(t)

def _find_master_episode_table(soup: BeautifulSoup):
    for tbl in soup.find_all("table"):
        header_tr = None
        for tr_try in tbl.find_all("tr"):
            if tr_try.find("th"):
                header_tr = tr_try
                break
        if not header_tr:
            continue
        ths = header_tr.find_all("th")
        th_texts = [_cell_text(th) for th in ths]
        if _table_has_headers(th_texts, ["Season", "Episode", "Air", "Title"]):
            return tbl
    return None

def _build_colmap(header_cells: List[str]) -> Dict[str, Optional[int]]:
    hdrs = [h.lower() for h in header_cells]
    def find(*patterns) -> Optional[int]:
        for i, h in enumerate(hdrs):
            for p in patterns:
                if re.search(p, h, re.I):
                    return i
        return None
    return {
        "season":   find(r"\bseason\b"),
        "overall":  find(r"\bno\.\s*overall\b", r"\boverall\b"),
        "in_season":find(r"\bno\.\s*in\s*season\b", r"\bepisode\s*no\.\b", r"\bep\.?\b", r"\bepisode\b"),
        "title":    find(r"\bepisode\s*title\b", r"\btitle\b"),
        "air_date": find(r"\bair\s*date\b", r"\boriginal"),
        "type":     find(r"\btype\b", r"\bepisode\s*type\b"),
        "viewers":  find(r"\bu\.?s\.?\s*viewers", r"\bviewers\b", r"\bmillions\b"),
    }

def _build_colmap_seasonpage(header_cells: List[str]) -> Dict[str, Optional[int]]:
    # Normalize once
    hdrs = [h.lower().strip() for h in header_cells]

    def find(*patterns) -> Optional[int]:
        for i, h in enumerate(hdrs):
            for p in patterns:
                if re.search(p, h, re.I):
                    return i
        return None

    # Headers on older/mixed pages vary a lot; include wide patterns
    return {
        "overall":  find(
            r"\boverall\b",
            r"\bno\.\s*overall\b",
            r"\boverall\s*no\.?\b",
            r"\boverall\s*episode\b",
        ),
        "in_season": find(
            r"\bno\.\s*in\s*season\b",
            r"\bepisode\s*no\.\b",
            r"\bep(?:isode)?\.?\b",
            r"\bin\s*season\b",
            r"^#\s*$",                 # bare column with #
            r"\bepisode\b"
        ),
        "title": find(
            r"\bepisode\s*title\b",
            r"\btitle\b",
            r"\bepisode\s*name\b",
            r"\bname\b"
        ),
        "air_date": find(
            r"\boriginal\s*u\.?s\.?\s*air\s*date\b",
            r"\bu\.?s\.?\s*air\s*date\b",
            r"\boriginal\s*air\s*date\b",
            r"\bair\s*date\b",
            r"\brelease\s*date\b"
        ),
        "type": find(
            r"\btype\b",
            r"\bepisode\s*type\b",
            r"\bspecial\b",
            r"\bcategory\b"
        ),
        "viewers": find(
            r"\bu\.?s\.?\s*viewers",
            r"\bviewers\b",
            r"\bmillions\b",
            r"\bratings\b"
        ),
    }

def _parse_master_table_into_dict(soup: BeautifulSoup, source_url: str) -> Dict[str, List[Dict]]:
    target_table = _find_master_episode_table(soup)
    if not target_table:
        return {}
    matrix = _normalize_table_rows(target_table)
    if not matrix or len(matrix) < 2:
        return {}

    header = matrix[0]
    colmap = _build_colmap(header)
    if colmap["season"] is None or colmap["title"] is None:
        return {}

    episodes_by_season: Dict[str, List[Dict]] = {}
    current_season_number: Optional[int] = None
    current_season_label: Optional[str] = None
    per_season_counter: Dict[int, int] = {}

    for row in matrix[1:]:
        if not any(cell.strip() for cell in row):
            continue

        season_cell = row[colmap["season"]] if colmap["season"] is not None else ""
        if season_cell.strip():
            current_season_label = season_cell.strip()
            current_season_number = _season_number_from_cell(current_season_label)

        if current_season_number is None:
            continue

        raw_title = row[colmap["title"]] if colmap["title"] is not None else ""
        title = _clean_quoted(raw_title)

        raw_air = row[colmap["air_date"]] if colmap["air_date"] is not None else ""
        if _looks_like_notes_blob(raw_air) or _looks_like_name_garbage(raw_air):
            raw_air = ""
        air_date = _clean_air_date(raw_air)

        if not title and not air_date:
            continue

        ep_overall = _to_int(row[colmap["overall"]]) if colmap["overall"] is not None else None
        ep_in_season = _to_int(row[colmap["in_season"]]) if colmap["in_season"] is not None else None
        if ep_in_season is None:
            per_season_counter.setdefault(current_season_number, 0)
            per_season_counter[current_season_number] += 1
            ep_in_season = per_season_counter[current_season_number]
        else:
            per_season_counter[current_season_number] = max(
                per_season_counter.get(current_season_number, 0), ep_in_season
            )

        ep_type = row[colmap["type"]].strip() if colmap["type"] is not None else None
        ep_type = ep_type or None

        viewers = _to_float(row[colmap["viewers"]]) if colmap["viewers"] is not None else None

        key = str(current_season_number)
        episodes_by_season.setdefault(key, []).append({
            "season_number": current_season_number,
            "season_label": current_season_label,
            "episode_in_season": ep_in_season,
            "overall_episode_number": ep_overall,
            "title": title or None,
            "air_date": air_date,
            "episode_type": ep_type,
            "us_viewers_millions": viewers,
            "source_url": source_url,
        })

    # sort & dedupe by episode_in_season (prefer records with real title/date)
    for key, eps in episodes_by_season.items():
        tmp = {}
        for rec in eps:
            epn = rec.get("episode_in_season")
            if epn not in tmp:
                tmp[epn] = rec
            else:
                old = tmp[epn]
                better = (
                    (rec.get("air_date") and not old.get("air_date")) or
                    (rec.get("title") and not old.get("title"))
                )
                if better:
                    tmp[epn] = rec
        episodes_by_season[key] = sorted(tmp.values(), key=lambda x: x.get("episode_in_season") or 0)
    return episodes_by_season

def _find_episodes_section(soup: BeautifulSoup):
    def looks_like(s: str) -> bool:
        t = s.lower()
        return any(k in t for k in [
            "episode", "episodes", "episode guide", "episode list", "episode summary", "ep guide"
        ])
    for tag in soup.find_all(["h2", "h3", "h4"]):
        if looks_like(_extract_text(tag)):
            return tag
    for tag in soup.find_all(["h2", "h3", "h4"]):
        span = tag.find("span", {"id": True})
        if span and looks_like(span.get("id", "")):
            return tag
    return None

def _heading_level(tag) -> int:
    if not tag or not tag.name or not tag.name.startswith("h"):
        return 7
    try:
        return int(tag.name[1])
    except Exception:
        return 7

def _collect_section_nodes(start_heading):
    nodes = []
    level = _heading_level(start_heading)
    node = start_heading.find_next_sibling()
    while node:
        if node.name and node.name.startswith("h"):
            if _heading_level(node) <= level:
                break
        nodes.append(node)
        node = node.find_next_sibling()
    return nodes

def _find_episode_like_links(soup: BeautifulSoup) -> List[str]:
    """
    Return up to ~40 unique internal links that *look* like episode list/episode pages.
    We widen the net because older seasons often use subpages such as ".../Episodes",
    ".../Episode_guide", etc.
    """
    hrefs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("/wiki/"):
            continue
        label = _extract_text(a).lower()
        href_l = href.lower()

        looks_episodeish = any(k in label for k in [
            "episode", "episodes", "episode guide", "episode list", "list of episodes"
        ]) or any(k in href_l for k in [
            "episode", "episodes", "episode_guide", "episode_list", "list_of_episodes"
        ])

        if looks_episodeish:
            hrefs.append(href)

    # keep unique, preserve order
    seen = set()
    out = []
    for h in hrefs:
        if h not in seen:
            seen.add(h)
            out.append(h)

    # allow more links – older seasons often need the deeper subpage
    return out[:40]

def _fetch_internal_path(path: str) -> Tuple[str, Optional[str]]:
    title = path.split("/wiki/")[-1]
    title = requests.utils.unquote(title.replace("_", " "))
    return _mediawiki_parse_html(title)

def _find_episodes_tables_on_season_page(soup: BeautifulSoup) -> List:
    start = _find_episodes_section(soup)
    if start:
        nodes = _collect_section_nodes(start)
        tables = []
        for n in nodes:
            if n.name == "table" and "wikitable" in (n.get("class") or []):
                tables.append(n)
        if tables:
            return tables
    return [t for t in soup.find_all("table") if "wikitable" in (t.get("class") or [])]

def _parse_episode_list_blocks(soup: BeautifulSoup, season_number: int, source_url: str) -> List[Dict]:
    """
    Some season pages (or their subpages) list episodes in <ul>/<ol> or paragraphs, not tables.
    We scan list items/paragraphs for patterns like:
       - Ep 3 — "Title" — September 29, 2011
       - 3. "Title" (September 29, 2011)
       - "Title" — Original U.S. air date: September 29, 2011
    This returns a *best effort* extraction (title + air_date) and assigns ep numbers incrementally
    if we can't read them explicitly.
    """
    results: List[Dict] = []

    # Candidates: lists and standalone paragraphs in the episode section
    def get_episode_section_nodes():
        start = _find_episodes_section(soup)
        if start:
            return _collect_section_nodes(start)
        return soup.find_all(["ul", "ol", "p", "dl"])

    nodes = get_episode_section_nodes()

    # regex patterns
    re_epnum_leading = re.compile(r"^\s*(?:ep(?:isode)?\.?\s*)?(\d{1,2})\s*(?:[–—\-\.]|:)", re.I)
    re_title_quoted = re.compile(r"“([^”]+)”|\"([^\"]+)\"|\'([^\']+)\'")
    re_date_paren = re.compile(r"\(([^)]+)\)")
    re_date_label = re.compile(r"(?:original\s*u\.?s\.?\s*air\s*date|air\s*date|release\s*date)\s*[:\-]\s*(.+)$", re.I)

    def maybe_add(ep_in: Optional[int], title: Optional[str], date_candidate: Optional[str]):
        title = _clean_quoted(title or "")
        air_date = _clean_air_date(date_candidate or "")
        if not title and not air_date:
            return
        results.append({
            "season_number": season_number,
            "episode_in_season": ep_in,
            "overall_episode_number": None,
            "title": title or None,
            "air_date": air_date,
            "episode_type": None,
            "us_viewers_millions": None,
            "source_url": source_url,
        })

    # Parse list items and paragraphs
    counter = 0
    for node in nodes:
        items = []
        if node.name in ("ul", "ol"):
            items = node.find_all("li", recursive=False)
        elif node.name in ("p", "dl"):
            items = [node]

        for it in items:
            txt = _extract_text(it)
            if not txt:
                continue
            # Skip obvious note blobs
            if _looks_like_notes_blob(txt):
                continue

            ep_in = None
            mnum = re_epnum_leading.search(txt)
            if mnum:
                try:
                    ep_in = int(mnum.group(1))
                except Exception:
                    ep_in = None

            # Title: prefer quoted
            mt = re_title_quoted.search(txt)
            title = None
            if mt:
                title = mt.group(1) or mt.group(2) or mt.group(3)
            else:
                # try italics <i> in HTML
                i = it.find("i")
                if i:
                    title = _extract_text(i)

            # Date: from parentheses or "Air date: ..."
            date_candidate = None
            mparen = re_date_paren.search(txt)
            if mparen and _is_valid_air_date(mparen.group(1).strip()):
                date_candidate = mparen.group(1).strip()
            else:
                mlabel = re_date_label.search(txt)
                if mlabel:
                    date_candidate = mlabel.group(1).strip()

            if ep_in is None:
                counter += 1
                ep_in = counter
            else:
                counter = max(counter, ep_in)

            maybe_add(ep_in, title, date_candidate)

    # de-dup per ep number (prefer rows that have a valid date + title)
    if results:
        tmp = {}
        for r in results:
            k = r["episode_in_season"]
            if k not in tmp:
                tmp[k] = r
            else:
                old = tmp[k]
                better = ((r.get("air_date") and not old.get("air_date")) or
                          (r.get("title") and not old.get("title")))
                if better:
                    tmp[k] = r
        results = [tmp[k] for k in sorted(tmp)]
    return results

def _parse_season_page_for_episodes(season_number: int, title_variants: List[str]) -> List[Dict]:
    def parse_soup_for_eps(soup: BeautifulSoup, source_url: str) -> List[Dict]:
        # Try tables first
        results: List[Dict] = []
        tables = _find_episodes_tables_on_season_page(soup)
        for tbl in tables:
            matrix = _normalize_table_rows(tbl)
            if not matrix or len(matrix) < 2:
                continue
            header = matrix[0]
            colmap = _build_colmap_seasonpage(header)
            if colmap["title"] is None and colmap["air_date"] is None:
                continue

            counter = 0
            for row in matrix[1:]:
                if not any(cell.strip() for cell in row):
                    continue

                raw_title = row[colmap["title"]] if colmap["title"] is not None and colmap["title"] < len(row) else ""
                title = _clean_quoted(raw_title)

                raw_air = row[colmap["air_date"]] if colmap["air_date"] is not None and colmap["air_date"] < len(row) else ""
                if _looks_like_notes_blob(raw_air) or _looks_like_name_garbage(raw_air):
                    raw_air = ""
                air_date = _clean_air_date(raw_air)

                if not title and not air_date:
                    continue

                ep_overall = _to_int(row[colmap["overall"]]) if colmap["overall"] is not None and colmap["overall"] < len(row) else None
                ep_in_season = _to_int(row[colmap["in_season"]]) if colmap["in_season"] is not None and colmap["in_season"] < len(row) else None
                if ep_in_season is None:
                    counter += 1
                    ep_in_season = counter
                else:
                    counter = max(counter, ep_in_season)

                ep_type = (row[colmap["type"]].strip() if colmap["type"] is not None and colmap["type"] < len(row) else None) or None
                viewers = _to_float(row[colmap["viewers"]]) if colmap["viewers"] is not None and colmap["viewers"] < len(row) else None

                results.append({
                    "season_number": season_number,
                    "episode_in_season": ep_in_season,
                    "overall_episode_number": ep_overall,
                    "title": title or None,
                    "air_date": air_date,
                    "episode_type": ep_type,
                    "us_viewers_millions": viewers,
                    "source_url": source_url,
                })

        if results:
            # Clean and return table results
            tmp = {}
            for rec in results:
                k = rec["episode_in_season"]
                if k not in tmp:
                    tmp[k] = rec
                else:
                    old = tmp[k]
                    better = ((rec.get("air_date") and not old.get("air_date")) or
                              (rec.get("title") and not old.get("title")))
                    if better:
                        tmp[k] = rec
            return [tmp[k] for k in sorted(tmp)]

        # If no tables, try list/paragraph extraction
        list_results = _parse_episode_list_blocks(soup, season_number, source_url)
        return list_results

    # Pass 1: season main page
    for title in title_variants:
        _sleep()
        html, source_url = _mediawiki_parse_html(title)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        eps = parse_soup_for_eps(soup, source_url)
        if eps:
            return sorted(eps, key=lambda x: (x.get("episode_in_season") or 0))

        # Pass 2: follow any episode-like subpages and parse (tables OR lists)
        for href in _find_episode_like_links(soup):
            _sleep()
            sub_html, sub_url = _fetch_internal_path(href)
            if not sub_html:
                continue
            sub_soup = BeautifulSoup(sub_html, "html.parser")
            eps2 = parse_soup_for_eps(sub_soup, sub_url or source_url)
            if eps2:
                return sorted(eps2, key=lambda x: (x.get("episode_in_season") or 0))

    return []

def _find_episode_rows_with_links_on_season_page(season_title: str) -> List[Tuple[int, str]]:
    """
    Returns a list of (episode_in_season, episode_page_title) by parsing the season page's episode table
    and taking the best link from the Title cell (prefers '(episode)' pages).
    """
    html, _ = _mediawiki_parse_html(season_title)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    tables = _find_episodes_tables_on_season_page(soup)
    out: List[Tuple[int, str]] = []

    def pick_episode_link(title_cell) -> Optional[str]:
        # prefer links that point to "(episode)" pages
        for a in title_cell.find_all("a", href=True, title=True):
            ttl = a.get("title") or ""
            href = a.get("href") or ""
            if "(episode)" in ttl.lower() or "(episode)" in href.lower():
                return requests.utils.unquote(href.split("/wiki/")[-1]).replace("_", " ")
        # else take first link text and try appending " (episode)"
        a = title_cell.find("a", href=True, title=True)
        if not a:
            return None
        raw_t = _extract_text(a)
        if not raw_t:
            return None
        return f"{raw_t} (episode)"

    for tbl in tables:
        header_tr = None
        for tr in tbl.find_all("tr"):
            if tr.find("th"):
                header_tr = tr
                break
        if not header_tr:
            continue
        header_cells = [_cell_text(th) for th in header_tr.find_all("th")]
        cmap = _build_colmap_seasonpage(header_cells)
        if cmap["title"] is None:
            continue

        ep_counter = 0
        for tr in tbl.find_all("tr")[1:]:
            tds = tr.find_all(["td", "th"])
            if not tds:
                continue
            if cmap["title"] >= len(tds):
                continue
            title_cell = tds[cmap["title"]]

            # episode number (fallback to counter)
            ep_in = None
            if cmap["in_season"] is not None and cmap["in_season"] < len(tds):
                ep_in = _to_int(_cell_text(tds[cmap["in_season"]]))
            if ep_in is None:
                ep_counter += 1
                ep_in = ep_counter

            page_title = pick_episode_link(title_cell)
            if not page_title:
                continue

            out.append((ep_in, page_title))

    dedup = {}
    for ep_in, title in out:
        dedup.setdefault(ep_in, title)
    return sorted(dedup.items(), key=lambda x: x[0])

def _find_episodes_tables_on_season_page_or_fallback(sn: int) -> List[Tuple[int, str]]:
    for title in _season_title_variants(sn):
        links = _find_episode_rows_with_links_on_season_page(title)
        if links:
            return links
    return []

def _find_master_episodes_html() -> Tuple[str, Optional[str]]:
    _sleep()
    return _mediawiki_parse_html("List of Survivor (U.S.) episodes")

def fetch_episodes_by_season() -> Dict[str, List[Dict]]:
    # Try the master “List of Survivor (U.S.) episodes” page
    html, url = _find_master_episodes_html()
    master_result: Dict[str, List[Dict]] = {}
    if html:
        soup = BeautifulSoup(html, "html.parser")
        master_result = _parse_master_table_into_dict(soup, url)

    episodes_by_season: Dict[str, List[Dict]] = dict(master_result) if master_result else {}

    # IMPORTANT: sweep every season number in the configured range,
    # even if the season info page failed earlier.
    for sn in range(settings.MIN_SEASON, settings.MAX_SEASON_GUESS + 1):
        key = str(sn)
        have_master = key in episodes_by_season and len(episodes_by_season[key]) > 0
        if have_master:
            episodes_by_season[key].sort(key=lambda x: (x.get("episode_in_season") or 0))
            continue

        variants = _season_title_variants(sn)
        eps = _parse_season_page_for_episodes(sn, variants)
        if eps:
            eps.sort(key=lambda x: (x.get("episode_in_season") or 0))
            episodes_by_season[key] = eps

    # Final tidy: drop keys that ended up empty (no episodes found at all)
    episodes_by_season = {k: v for k, v in episodes_by_season.items() if v}

    return episodes_by_season

# ------------------------------------------------------------
# Episode details (immunity winners, who left, advantage events)
# ------------------------------------------------------------

def _parse_episode_infobox(soup: BeautifulSoup) -> Dict[str, List[str]]:
    info = {"immunity_winners": [], "eliminated": []}
    infobox = None
    for tbl in soup.find_all("table"):
        classes = " ".join(tbl.get("class") or [])
        if "infobox" in classes:
            infobox = tbl
            break
    if not infobox:
        return info

    def grab(label_patterns: List[str]) -> List[str]:
        vals: List[str] = []
        for tr in infobox.find_all("tr"):
            th = tr.find("th")
            if not th:
                continue
            key = _extract_text(th).lower()
            if any(re.search(p, key, re.I) for p in label_patterns):
                td = tr.find("td")
                if not td:
                    continue
                links = [_extract_text(a) for a in td.find_all("a")]
                txt = _extract_text(td)
                candidates = links if links else re.split(r"[•,;/\-]| and ", txt)
                vals.extend([v.strip() for v in candidates if v.strip()])
        seen = set()
        out = []
        for v in vals:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    info["immunity_winners"] = grab([
        r"\bindividual\s*immunity\b",
        r"\btribal\s*immunity\b",
        r"\bimmunity\b",
    ])

    eliminated = grab([
        r"\bvoted\s*out\b",
        r"\beliminated\b",
        r"\bmedic(?:ally)?\s*evac",
        r"\bquit\b",
        r"\boutcome\b",
    ])
    info["eliminated"] = eliminated

    return info

_ADVANTAGE_KEYWORDS = [
    "idol", "beware", "knowledge is power", "kip", "nullifier", "extra vote",
    "steal a vote", "shot in the dark", "sitd", "safety without power",
    "bank your vote", "inheritance", "journey", "advantage", "amule", "amulet"
]

def _extract_advantage_events(soup: BeautifulSoup) -> List[Dict[str, str]]:
    events: List[Dict[str, str]] = []

    def looks_like_header(t: str) -> bool:
        t = t.lower()
        return any(k in t for k in ["note", "notes", "summary", "episode notes"])

    header = None
    for h in soup.find_all(["h2", "h3", "h4"]):
        if looks_like_header(_extract_text(h)):
            header = h
            break

    nodes = []
    if header:
        level = _heading_level(header)
        node = header.find_next_sibling()
        while node:
            if node.name and node.name.startswith("h") and _heading_level(node) <= level:
                break
            nodes.append(node)
            node = node.find_next_sibling()
    else:
        nodes = soup.find_all(["p", "li"])

    def tag_for(text: str) -> str:
        t = text.lower()
        for k in _ADVANTAGE_KEYWORDS:
            if k in t:
                return k
        return "event"

    for n in nodes:
        if n is None:
            continue
        txt = _extract_text(n)
        if not txt:
            continue
        tl = txt.lower()
        if any(k in tl for k in _ADVANTAGE_KEYWORDS):
            events.append({"text": txt, "tag": tag_for(txt)})

    seen = set()
    out = []
    for e in events:
        key = e["text"]
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out

def _episode_details_from_page(title: str) -> Dict:
    _sleep()
    html, url = _mediawiki_parse_html(title)
    if not html and "(episode)" not in title.lower():
        html, url = _mediawiki_parse_html(f"{title} (episode)")
    if not html:
        return {"source_url": None, "immunity_winners": [], "eliminated": [], "advantage_events": []}
    soup = BeautifulSoup(html, "html.parser")
    inf = _parse_episode_infobox(soup)
    adv = _extract_advantage_events(soup)
    return {
        "source_url": url,
        "immunity_winners": inf.get("immunity_winners", []),
        "eliminated": inf.get("eliminated", []),
        "advantage_events": adv,
    }

def _season_episode_links(sn: int) -> List[Tuple[int, str]]:
    for title in _season_title_variants(sn):
        links = _find_episode_rows_with_links_on_season_page(title)
        if links:
            return links
    return []

def enrich_episode_details(episodes_by_season: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
    index: Dict[int, Dict[int, Dict]] = {}
    for skey, eps in episodes_by_season.items():
        try:
            sn = int(skey)
        except Exception:
            continue
        index[sn] = {}
        for rec in eps:
            epn = rec.get("episode_in_season")
            if isinstance(epn, int):
                index[sn][epn] = rec

    for skey, eps in episodes_by_season.items():
        try:
            sn = int(skey)
        except Exception:
            continue

        links = _season_episode_links(sn)  # [(ep_in, page_title)]
        if not links:
            continue

        for ep_in, page_title in links:
            rec = index.get(sn, {}).get(ep_in)
            if not rec:
                continue
            air_date = _clean_air_date(rec.get("air_date"))
            if not air_date:
                continue  # skip future/invalid rows
            details = _episode_details_from_page(page_title)
            if details.get("source_url"):
                rec["episode_page_url"] = details["source_url"]
            if details.get("immunity_winners"):
                rec["immunity_winners"] = details["immunity_winners"]
            if details.get("eliminated"):
                rec["eliminated"] = details["eliminated"]
            if details.get("advantage_events"):
                rec["advantage_events"] = details["advantage_events"]

    return episodes_by_season
