import re
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from settings import settings

# ------------------------------------------------------------
# HTTP session + helpers
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
    canonical = settings.WIKI_BASE + "/wiki/" + requests.utils.quote(
        data["parse"]["title"].replace(" ", "_")
    )
    return html, canonical

def _extract_text(el) -> str:
    if not el:
        return ""
    return " ".join(el.get_text(" ", strip=True).split())

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
    seasons: List[Dict] = []
    max_seen = 0
    for n in range(settings.MIN_SEASON, settings.MAX_SEASON_GUESS + 1):
        data = fetch_one_season(n)
        if data:
            seasons.append(data)
            max_seen = n
        else:
            if n > max_seen + 2:
                break
    seasons.sort(key=lambda x: x["season_number"])
    return seasons

# ------------------------------------------------------------
# Episodes scraping (master table + per-season fallback)
# ------------------------------------------------------------

def _table_has_headers(th_texts: List[str], required: List[str]) -> bool:
    lower = [t.lower() for t in th_texts]
    return all(any(req.lower() in h for h in lower) for req in required)

def _clean_quoted(s: str) -> str:
    return (s or "").strip().strip('“”"\' ').strip()

def _cell_text(cell) -> str:
    for br in cell.find_all("br"):
        br.replace_with(" / ")
    return _extract_text(cell)

def _parse_span(value, default=1, hard_cap=20) -> int:
    if value is None or value == "":
        return default
    s = str(value)
    m = re.search(r"\d+", s)
    if not m:
        return default
    n = int(m.group(0))
    if n > hard_cap:
        return 1
    return max(1, n)

def _normalize_table_rows(table) -> List[List[str]]:
    rows = table.find_all("tr")
    if not rows:
        return []

    header_tr = None
    header_cells = []
    header_idx = -1
    for i, tr in enumerate(rows):
        ths = tr.find_all("th")
        if ths:
            header_tr = tr
            header_idx = i
            header_cells = [_cell_text(th) for th in ths]
            break
    if header_tr is None:
        return []

    num_cols = 0
    for th in header_tr.find_all("th"):
        cspan = _parse_span(th.get("colspan"), default=1, hard_cap=20)
        num_cols += cspan if cspan >= 1 else 1

    matrix: List[List[str]] = []
    header_row: List[str] = []
    for th in header_tr.find_all("th"):
        text = _cell_text(th)
        cspan = _parse_span(th.get("colspan"), default=1, hard_cap=20)
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
        tds = tr.find_all(["td", "th"])
        td_iter = iter(tds)
        col = 0
        while col < num_cols:
            if pending[col] is not None:
                val, rem = pending[col]
                row.append(val)
                rem -= 1
                pending[col] = (val, rem) if rem > 0 else None
                col += 1
                continue
            try:
                cell = next(td_iter)
            except StopIteration:
                row.extend([""] * (num_cols - len(row)))
                break
            text = _cell_text(cell)
            rspan = _parse_span(cell.get("rowspan"), default=1, hard_cap=20)
            cspan = _parse_span(cell.get("colspan"), default=1, hard_cap=20)
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

    for tr in rows[header_idx + 1:]:
        if not tr.find_all(["td", "th"]):
            continue
        matrix.append(build_row_from_tr(tr))
    return matrix

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
        title = _clean_quoted(row[colmap["title"]]) if colmap["title"] is not None else None
        air_date = row[colmap["air_date"]].strip() if colmap["air_date"] is not None else None
        air_date = air_date or None
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
        rec = {
            "season_number": current_season_number,
            "season_label": current_season_label,
            "episode_in_season": ep_in_season,
            "overall_episode_number": ep_overall,
            "title": title or None,
            "air_date": air_date,
            "episode_type": ep_type,
            "us_viewers_millions": viewers,
            "source_url": source_url,
        }
        key = str(current_season_number)
        episodes_by_season.setdefault(key, []).append(rec)
    for key, eps in episodes_by_season.items():
        eps.sort(key=lambda x: (x.get("episode_in_season") or 0))
    return episodes_by_season

# -------- FALLBACK: individual season pages (tables + lists + subpages) --------

def _heading_level(tag) -> int:
    if not tag or not tag.name or not tag.name.startswith("h"):
        return 7
    try:
        return int(tag.name[1])
    except Exception:
        return 7

def _collect_section_nodes(start_heading):
    """Collect siblings after start_heading until the next heading of same or higher level."""
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

def _find_episodes_section(soup: BeautifulSoup):
    """Find a heading (h2/h3/h4) that looks like the Episodes section."""
    def looks_like(s: str) -> bool:
        t = s.lower()
        return any(k in t for k in [
            "episode", "episodes", "episode guide", "episode list", "episode summary", "ep guide"
        ])
    for tag in soup.find_all(["h2", "h3", "h4"]):
        if looks_like(_extract_text(tag)):
            return tag
    # sometimes the section has an anchor id
    for tag in soup.find_all(["h2", "h3", "h4"]):
        span = tag.find("span", {"id": True})
        if span and looks_like(span.get("id", "")):
            return tag
    return None

def _parse_list_items(ul_tag) -> List[Dict]:
    """Parse <ul><li> blocks under episodes sections."""
    items: List[Dict] = []
    for li in ul_tag.find_all("li", recursive=False):
        text = _extract_text(li)
        if not text:
            continue
        # common pattern: Title – Month DD, YYYY
        m = re.search(r'“?([^”"]+)”?\s*[–-]\s*([A-Za-z]+ \d{1,2}, \d{4})', text)
        title = None
        air_date = None
        if m:
            title = m.group(1).strip()
            air_date = m.group(2).strip()
        else:
            # try a weaker split: "Title (Month DD, YYYY)" or "Title - date"
            m2 = re.search(r'“?([^”"]+)”?\s*\(?(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\)?', text)
            if m2:
                title = m2.group(1).strip()
                air_date = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', text).group(0)
            else:
                title = text
        items.append({"title": title, "air_date": air_date, "raw": text})
    return items

def _find_episode_like_links(soup: BeautifulSoup) -> List[str]:
    """Find internal links that look like episode list pages."""
    hrefs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("/wiki/"):
            continue
        label = _extract_text(a).lower()
        href_l = href.lower()
        if any(k in label for k in ["episode", "episodes", "episode guide", "episode list"]) or \
           any(k in href_l for k in ["episode", "episodes", "episode_guide", "episode_list"]):
            hrefs.append(href)
    # dedupe preserve order
    seen = set()
    out = []
    for h in hrefs:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:5]

def _fetch_internal_path(path: str) -> Tuple[str, Optional[str]]:
    """Fetch a /wiki/... internal path through the API parse endpoint."""
    title = path.split("/wiki/")[-1]
    title = requests.utils.unquote(title.replace("_", " "))
    return _mediawiki_parse_html(title)

def _find_episodes_tables_on_season_page(soup: BeautifulSoup) -> List:
    """Find wikitables likely to be episode lists under an Episodes section if possible; else all wikitables."""
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

def _build_colmap_seasonpage(header_cells: List[str]) -> Dict[str, Optional[int]]:
    hdrs = [h.lower() for h in header_cells]
    def find(*patterns) -> Optional[int]:
        for i, h in enumerate(hdrs):
            for p in patterns:
                if re.search(p, h, re.I):
                    return i
        return None
    return {
        "overall":  find(r"\boverall\b", r"\bno\.\s*overall\b"),
        "in_season":find(r"\bno\.\s*in\s*season\b", r"\bepisode\s*no\.\b", r"\bep\.?\b", r"\bepisode\b"),
        "title":    find(r"\btitle\b"),
        "air_date": find(r"\bair\s*date\b", r"\boriginal"),
        "type":     find(r"\btype\b", r"\bepisode\s*type\b"),
        "viewers":  find(r"\bu\.?s\.?\s*viewers", r"\bviewers\b", r"\bmillions\b"),
    }

def _parse_season_page_for_episodes(season_number: int, title_variants: List[str]) -> List[Dict]:
    """
    Try:
      1) Season page: Episodes section (tables + lists)
      2) If not found, follow internal links that look like episodes/guide pages and parse those.
    """
    # -------- helper to parse a soup into episode rows (tables + lists) --------
    def parse_soup_for_eps(soup: BeautifulSoup, source_url: str) -> List[Dict]:
        results: List[Dict] = []

        # (A) Priority: tables under the Episodes section (or any wikitables if not found)
        tables = _find_episodes_tables_on_season_page(soup)
        best_per_season_counter = 0
        for tbl in tables:
            matrix = _normalize_table_rows(tbl)
            if not matrix or len(matrix) < 2:
                continue
            header = matrix[0]
            colmap = _build_colmap_seasonpage(header)
            if colmap["title"] is None and colmap["air_date"] is None:
                continue  # not an episode list table
            per_season_counter = 0
            for row in matrix[1:]:
                if not any(cell.strip() for cell in row):
                    continue
                title_txt = _clean_quoted(row[colmap["title"]]) if colmap["title"] is not None else None
                air_date = row[colmap["air_date"]].strip() if colmap["air_date"] is not None else None
                air_date = air_date or None
                ep_overall = _to_int(row[colmap["overall"]]) if colmap["overall"] is not None else None
                ep_in_season = _to_int(row[colmap["in_season"]]) if colmap["in_season"] is not None else None
                ep_type = row[colmap["type"]].strip() if colmap["type"] is not None else None
                ep_type = ep_type or None
                viewers = _to_float(row[colmap["viewers"]]) if colmap["viewers"] is not None else None

                if not title_txt and not air_date:
                    continue
                if ep_in_season is None:
                    per_season_counter += 1
                    ep_in_season = per_season_counter
                else:
                    per_season_counter = max(per_season_counter, ep_in_season)

                results.append({
                    "season_number": season_number,
                    "episode_in_season": ep_in_season,
                    "overall_episode_number": ep_overall,
                    "title": title_txt or None,
                    "air_date": air_date,
                    "episode_type": ep_type,
                    "us_viewers_millions": viewers,
                    "source_url": source_url,
                })
            best_per_season_counter = max(best_per_season_counter, per_season_counter)

        # (B) If no tables produced results, parse lists under the Episodes section
        if not results:
            start = _find_episodes_section(soup)
            if start:
                nodes = _collect_section_nodes(start)
                li_items: List[Dict] = []
                for n in nodes:
                    if n.name in ["ul", "ol"]:
                        li_items.extend(_parse_list_items(n))
                if li_items:
                    # assign in-season numbers in order
                    for i, item in enumerate(li_items, start=1):
                        results.append({
                            "season_number": season_number,
                            "episode_in_season": i,
                            "overall_episode_number": None,
                            "title": item.get("title"),
                            "air_date": item.get("air_date"),
                            "episode_type": None,
                            "us_viewers_millions": None,
                            "source_url": source_url,
                        })

        return results

    # -------- 1) Season page itself --------
    for title in title_variants:
        _sleep()
        html, source_url = _mediawiki_parse_html(title)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        eps = parse_soup_for_eps(soup, source_url)
        if eps:
            eps.sort(key=lambda x: (x.get("episode_in_season") or 0))
            return eps

        # -------- 2) Follow likely "Episode Guide" / "Episodes" links and try again --------
        for href in _find_episode_like_links(soup):
            _sleep()
            sub_html, sub_url = _fetch_internal_path(href)
            if not sub_html:
                continue
            sub_soup = BeautifulSoup(sub_html, "html.parser")
            eps2 = parse_soup_for_eps(sub_soup, sub_url or source_url)
            if eps2:
                eps2.sort(key=lambda x: (x.get("episode_in_season") or 0))
                return eps2

    return []

def fetch_episodes_by_season() -> Dict[str, List[Dict]]:
    """
    1) Try consolidated 'List of Survivor (U.S.) episodes' page.
    2) For any seasons missing (or empty), fall back to scraping their individual season page
       (tables, lists, or a linked 'Episode Guide' sub-page).
    """
    _sleep()
    html, url = _mediawiki_parse_html("List of Survivor (U.S.) episodes")
    master_result: Dict[str, List[Dict]] = {}
    if html:
        soup = BeautifulSoup(html, "html.parser")
        master_result = _parse_master_table_into_dict(soup, url)

    seasons = fetch_all_seasons()
    episodes_by_season: Dict[str, List[Dict]] = dict(master_result) if master_result else {}

    for s in seasons:
        sn = int(s["season_number"])
        key = str(sn)
        if key in episodes_by_season and episodes_by_season[key]:
            continue
        variants = _season_title_variants(sn)
        eps = _parse_season_page_for_episodes(sn, variants)
        if eps:
            episodes_by_season[key] = eps

    for key, eps in episodes_by_season.items():
        eps.sort(key=lambda x: (x.get("episode_in_season") or 0))

    return episodes_by_season
