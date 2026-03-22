#!/usr/bin/env python3

import argparse
import csv
import html
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import datetime
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


BASE_URL = "https://www.abstractsonline.com"
API_BASE = f"{BASE_URL}/oe3"
MEETING_ID = "21436"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
BACKPACK_USERNAME = "backpack"
BACKPACK_PASSWORD = "89j34jks98cnjks989p;nfs44"

DEFAULT_KEYWORDS = [
    ("Xenium", "xenium"),
    ("Xenium + 5K", "xenium +5k"),
    ("Visium", "visium"),
    ("Chromium", "chromium"),
    ("10X", "10x"),
    ("CosMx", "cosmx"),
    ("GeoMx", "geomx"),
    ("MerFISH", "merfish"),
    ("nCounter", "ncounter"),
    ("CellScape", "cellscape"),
    ("PaintScape", "paintscape"),
    ("G4X", "g4x"),
    ("Beacon + Bruker", "beacon +bruker"),
    ("Bruker", "bruker"),
]

TERM_PATTERNS = OrderedDict(
    [
        ("Xenium", [r"\bxenium\b"]),
        ("Visium", [r"\bvisium\b"]),
        ("Chromium", [r"\bchromium\b"]),
        ("10X", [r"\b10x\b", r"\b10x genomics\b"]),
        ("CosMx", [r"\bcosmx\b"]),
        ("GeoMx", [r"\bgeomx\b"]),
        ("MerFISH", [r"\bmerfish\b"]),
        ("nCounter", [r"\bncounter\b"]),
        ("Bruker", [r"\bbruker\b"]),
        ("Beacon", [r"\bbeacon\b"]),
        ("CellScape", [r"\bcellscape\b"]),
        ("PaintScape", [r"\bpaintscape\b"]),
        ("G4X", [r"\bg4x\b"]),
    ]
)

PRODUCT_PATTERNS = OrderedDict(
    (label, patterns)
    for label, patterns in TERM_PATTERNS.items()
    if label not in {"Bruker", "10X"}
)

US_STATE_CODES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

US_STATE_NAMES = {name.upper(): code for code, name in US_STATE_CODES.items()}

COUNTRY_CODES = {
    "AUSTRALIA": ("Australia", "AUS"),
    "AUSTRIA": ("Austria", "AUT"),
    "BELGIUM": ("Belgium", "BEL"),
    "BRAZIL": ("Brazil", "BRA"),
    "CANADA": ("Canada", "CAN"),
    "CHINA": ("China", "CHN"),
    "DENMARK": ("Denmark", "DNK"),
    "FINLAND": ("Finland", "FIN"),
    "FRANCE": ("France", "FRA"),
    "GERMANY": ("Germany", "DEU"),
    "HONG KONG": ("Hong Kong", "HKG"),
    "INDIA": ("India", "IND"),
    "IRELAND": ("Ireland", "IRL"),
    "ISRAEL": ("Israel", "ISR"),
    "ITALY": ("Italy", "ITA"),
    "JAPAN": ("Japan", "JPN"),
    "KOREA, REPUBLIC OF": ("South Korea", "KOR"),
    "MEXICO": ("Mexico", "MEX"),
    "NETHERLANDS": ("Netherlands", "NLD"),
    "NORWAY": ("Norway", "NOR"),
    "SINGAPORE": ("Singapore", "SGP"),
    "SPAIN": ("Spain", "ESP"),
    "SWEDEN": ("Sweden", "SWE"),
    "SWITZERLAND": ("Switzerland", "CHE"),
    "TAIWAN": ("Taiwan", "TWN"),
    "UNITED KINGDOM": ("United Kingdom", "GBR"),
    "UNITED STATES": ("United States", "USA"),
    "USA": ("United States", "USA"),
    "US": ("United States", "USA"),
    "U.S.": ("United States", "USA"),
    "U.S.A.": ("United States", "USA"),
}

INSTITUTION_HINTS = (
    "university",
    "college",
    "school",
    "institute",
    "center",
    "centre",
    "hospital",
    "clinic",
    "laboratory",
    "lab",
    "cancer",
    "medical",
    "medicine",
    "brigham",
    "dana-farber",
    "memorial sloan",
    "md anderson",
    "bristol myers",
    "genomics",
    "biotools",
    "bruker",
    "dkfz",
    "epfl",
    "chuv",
)

TAG_RE = re.compile(r"<[^>]+>")
SUP_RE = re.compile(r"<sup>\s*\d+\s*</sup>", re.IGNORECASE)
DOUBLE_BREAK_RE = re.compile(r"<br\s*/?>\s*<br\s*/?>", re.IGNORECASE)
BREAK_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class KeywordSpec:
    label: str
    query: str


def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    value = WHITESPACE_RE.sub(" ", value)
    return value.strip()


def strip_tags(value: Optional[str]) -> str:
    if not value:
        return ""
    value = BREAK_RE.sub(" ", value)
    value = TAG_RE.sub("", value)
    return clean_text(value)


def parse_author_block(author_block: Optional[str]) -> Tuple[str, str]:
    if not author_block:
        return "", ""

    normalized = DOUBLE_BREAK_RE.sub("||AFFILIATIONS||", author_block)
    authors_html, affiliations_html = (normalized.split("||AFFILIATIONS||", 1) + [""])[:2]

    authors_html = SUP_RE.sub("", authors_html)
    authors = strip_tags(authors_html)

    affiliations_html = re.sub(r"<sup>\s*\d+\s*</sup>", " | ", affiliations_html, flags=re.IGNORECASE)
    affiliations = strip_tags(affiliations_html)
    affiliations = re.sub(r"\s*\|\s*", " | ", affiliations).strip(" |")
    affiliations = affiliations.replace(", |", " |")

    return authors, affiliations


def parse_disclosures(disclosure_block: Optional[str]) -> str:
    if not disclosure_block:
        return ""
    disclosure_block = BREAK_RE.sub(" | ", disclosure_block)
    disclosure_block = disclosure_block.replace("&nbsp;", " ")
    return clean_text(TAG_RE.sub("", html.unescape(disclosure_block))).strip("| ")


def additional_fields_to_map(items: Optional[Sequence[Dict[str, str]]]) -> Dict[str, str]:
    field_map: Dict[str, str] = {}
    for item in items or []:
        key = strip_tags(item.get("Key"))
        value = strip_tags(item.get("Value"))
        if key:
            field_map[key] = value
    return field_map


def natural_abstract_sort_key(value: str) -> Tuple[str, int]:
    match = re.match(r"([A-Za-z]*)(\d+)", value or "")
    if not match:
        return (value.lower(), sys.maxsize)
    prefix, number = match.groups()
    return (prefix.lower(), int(number))


def detect_matches(patterns: Dict[str, Sequence[str]], *fragments: str) -> List[str]:
    combined = clean_text(" ".join(fragment for fragment in fragments if fragment)).lower()
    matches: List[str] = []
    for label, candidates in patterns.items():
        if any(re.search(pattern, combined, re.IGNORECASE) for pattern in candidates):
            matches.append(label)
    return matches


def detect_products(*fragments: str) -> List[str]:
    return detect_matches(PRODUCT_PATTERNS, *fragments)


def table_slug(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def seed_products_from_keywords(source_keywords: Sequence[str]) -> List[str]:
    return detect_products(*source_keywords)


def split_affiliation_segments(affiliation: str) -> List[str]:
    return [clean_text(part) for part in affiliation.split("|") if clean_text(part)]


def extract_institution(segment: str) -> str:
    chunks = [clean_text(part) for part in segment.split(",") if clean_text(part)]
    if not chunks:
        return ""

    for chunk in reversed(chunks):
        lowered = chunk.lower()
        if any(hint in lowered for hint in INSTITUTION_HINTS):
            return chunk

    return chunks[0]


def extract_geography(segment: str) -> Tuple[str, str, str, str]:
    chunks = [clean_text(part) for part in segment.split(",") if clean_text(part)]
    if not chunks:
        return "", "", "", ""

    if len(chunks) >= 2:
        last_two = f"{chunks[-2]}, {chunks[-1]}".upper()
        if last_two in COUNTRY_CODES:
            country_name, country_code = COUNTRY_CODES[last_two]
            return country_name, country_code, "", ""

    last = chunks[-1].upper()
    if last in US_STATE_CODES:
        return "United States", "USA", last, US_STATE_CODES[last]
    if last in US_STATE_NAMES:
        return "United States", "USA", US_STATE_NAMES[last], last.title()
    if last in COUNTRY_CODES:
        country_name, country_code = COUNTRY_CODES[last]
        return country_name, country_code, "", ""

    return "", "", "", ""


class OasisClient:
    def __init__(self, meeting_id: str):
        self.meeting_id = meeting_id
        self.cookie_jar = CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar))
        self.backpack_id: Optional[str] = None

    def _headers(
        self,
        referer: str,
        include_backpack: bool = True,
        include_caller: bool = True,
    ) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "Origin": BASE_URL,
            "Referer": referer,
        }
        if include_caller:
            headers["Caller"] = "PP8"
        if include_backpack and self.backpack_id:
            headers["Backpack"] = self.backpack_id
        return headers

    def _request_json(
        self,
        url: str,
        *,
        data: Optional[dict] = None,
        method: Optional[str] = None,
        referer: str,
        include_backpack: bool = True,
        include_caller: bool = True,
        retries: int = 1,
    ) -> dict:
        request_data = None
        if data is not None:
            request_data = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=request_data,
            headers=self._headers(
                referer=referer,
                include_backpack=include_backpack,
                include_caller=include_caller,
            ),
            method=method,
        )

        try:
            with self.opener.open(req) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 401 and include_backpack and retries > 0:
                self.create_backpack()
                return self._request_json(
                    url,
                    data=data,
                    method=method,
                    referer=referer,
                    include_backpack=include_backpack,
                    include_caller=include_caller,
                    retries=retries - 1,
                )
            raise

    def create_backpack(self) -> str:
        referer = f"{BASE_URL}/pp8/#!/{self.meeting_id}/presentations/xenium/1"
        response = self._request_json(
            f"{API_BASE}/Backpack/create",
            data={"Username": BACKPACK_USERNAME, "Password": BACKPACK_PASSWORD},
            method="POST",
            referer=referer,
            include_backpack=False,
            include_caller=False,
        )
        self.backpack_id = response["ID"]
        return self.backpack_id

    def search_presentations(self, keyword: KeywordSpec) -> dict:
        referer = f"{BASE_URL}/pp8/#!/{self.meeting_id}/presentations/{urllib.parse.quote(keyword.query)}/1"
        response = self._request_json(
            f"{API_BASE}/Program/{self.meeting_id}/Search/New/presentation",
            data={"Phrase": keyword.query},
            method="POST",
            referer=referer,
        )
        status = clean_text(response.get("Status"))
        if status == "Complete":
            return response

        search_id = clean_text(response.get("SearchId"))
        for attempt in range(15):
            time.sleep(0.5 if attempt < 5 else 1.0)
            response = self._request_json(
                f"{API_BASE}/Program/{self.meeting_id}/Search/{search_id}",
                referer=referer,
            )
            if clean_text(response.get("Status")) == "Complete":
                return response

        raise RuntimeError(f"Search for {keyword.label} did not complete: {response}")

    def fetch_search_results(self, search_id: str, query: str, page_size: int = 200) -> List[dict]:
        referer = f"{BASE_URL}/pp8/#!/{self.meeting_id}/presentations/{urllib.parse.quote(query)}/1"
        url = (
            f"{API_BASE}/Program/{self.meeting_id}/Search/{search_id}/Results"
            f"?page=1&pagesize={page_size}&sort=1&order=asc"
        )
        payload = self._request_json(url, referer=referer)
        return payload.get("Results", [])

    def fetch_presentation(self, presentation_id: str, query: str) -> dict:
        referer = f"{BASE_URL}/pp8/#!/{self.meeting_id}/presentations/{urllib.parse.quote(query)}/1"
        return self._request_json(
            f"{API_BASE}/Program/{self.meeting_id}/Presentation/{presentation_id}",
            referer=referer,
        )


def normalize_presentation(detail: dict, source_keywords: Sequence[str]) -> dict:
    field_map = additional_fields_to_map(detail.get("AdditionalFields"))
    authors, affiliation = parse_author_block(detail.get("AuthorBlock"))
    abstract = strip_tags(detail.get("Abstract"))
    title = strip_tags(detail.get("Title"))
    session_title = strip_tags(detail.get("SessionTitle"))
    details_blob = " ".join(
        [
            title,
            abstract,
            authors,
            affiliation,
            session_title,
            field_map.get("Keywords", ""),
            " ".join(source_keywords),
        ]
    )
    products = detect_products(details_blob)
    for product in seed_products_from_keywords(source_keywords):
        if product not in products:
            products.append(product)

    return {
        "presentation_id": clean_text(detail.get("Id")),
        "presentation_number": clean_text(detail.get("PresentationNumber")),
        "title": title,
        "session_title": session_title,
        "session_id": clean_text(detail.get("SessionId")),
        "start": clean_text(detail.get("Start")),
        "end": clean_text(detail.get("End")),
        "posterboard_number": clean_text(detail.get("PosterboardNumber")),
        "authors": authors,
        "affiliation": affiliation,
        "abstract": abstract,
        "disclosures": parse_disclosures(detail.get("DisclosureBlock")),
        "presenter": strip_tags(detail.get("PresenterDisplayName")),
        "activity": strip_tags(detail.get("Activity")),
        "status": strip_tags(detail.get("Status")),
        "keywords_field": strip_tags(field_map.get("Keywords", "")),
        "topics_field": strip_tags(field_map.get("Topics", "")),
        "products": products,
        "source_keywords": list(source_keywords),
        "presentation_url": f"{BASE_URL}/pp8/#!/{MEETING_ID}/presentation/{clean_text(detail.get('Id'))}",
    }


def csv_join(values: Iterable[str]) -> str:
    return "; ".join(value for value in values if value)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_csv(path: Path, rows: Sequence[dict], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def render_table_rows(rows: Sequence[dict]) -> str:
    html_rows: List[str] = []
    for row in rows:
        products = csv_join(row["products"]) or "-"
        html_rows.append(
            "\n".join(
                [
                    "<tr>",
                    f"  <td class=\"mono\">{html.escape(row['presentation_number'])}</td>",
                    f"  <td>{html.escape(row['authors'])}</td>",
                    f"  <td>{html.escape(row['affiliation'])}</td>",
                    "  <td>",
                    f"    <div class=\"title\">{html.escape(row['title'])}</div>",
                    f"    <div class=\"meta\">{html.escape(row['start'])} | {html.escape(row['session_title'])}</div>",
                    f"    <div class=\"meta\"><a href=\"{html.escape(row['presentation_url'])}\">Open abstract</a></div>",
                    "  </td>",
                    f"  <td>{html.escape(products)}</td>",
                    "</tr>",
                ]
            )
        )
    return "\n".join(html_rows)


def render_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --blue: #1673c6;
      --blue-dark: #0f4d8b;
      --ink: #1f2d3d;
      --muted: #5f7187;
      --line: #c9d8e6;
      --panel: #f8fbfe;
      --accent: #4fae6a;
      --paper: #ffffff;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      font-family: "Aptos", "Segoe UI", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(22, 115, 198, 0.12), transparent 28%),
        linear-gradient(180deg, #f5fbff 0%, #ffffff 22%);
    }}

    .page {{
      width: min(1600px, calc(100vw - 48px));
      margin: 32px auto 48px;
      background: var(--paper);
      border: 1px solid #d8e4ef;
      box-shadow: 0 20px 50px rgba(15, 77, 139, 0.08);
    }}

    .hero {{
      padding: 28px 36px 22px;
      background:
        linear-gradient(135deg, #1d7bd0 0%, #0f5a9f 100%);
      color: #fff;
    }}

    .eyebrow {{
      font-size: 14px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      opacity: 0.9;
    }}

    h1 {{
      margin: 8px 0 0;
      font-size: 40px;
      line-height: 1.05;
      font-weight: 700;
    }}

    .content {{
      padding: 24px 36px 40px;
    }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px 42px;
      margin-bottom: 26px;
    }}

    .summary-item {{
      padding: 14px 0;
      border-bottom: 1px solid var(--line);
      font-size: 18px;
      line-height: 1.35;
    }}

    .summary-item strong {{
      color: var(--accent);
      font-weight: 700;
    }}

    .section-title {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 16px;
      margin: 28px 0 10px;
    }}

    .section-title h2 {{
      margin: 0;
      font-size: 28px;
      color: var(--blue-dark);
    }}

    .section-title .count {{
      color: var(--muted);
      font-size: 16px;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      background: var(--panel);
    }}

    thead th {{
      text-align: left;
      font-size: 13px;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      color: var(--blue-dark);
      padding: 12px 10px;
      border-top: 2px solid var(--line);
      border-bottom: 2px solid var(--line);
      background: #f4f9fd;
    }}

    tbody td {{
      vertical-align: top;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
      line-height: 1.4;
      background: #fff;
    }}

    th:nth-child(1), td:nth-child(1) {{ width: 10%; }}
    th:nth-child(2), td:nth-child(2) {{ width: 23%; }}
    th:nth-child(3), td:nth-child(3) {{ width: 19%; }}
    th:nth-child(4), td:nth-child(4) {{ width: 33%; }}
    th:nth-child(5), td:nth-child(5) {{ width: 15%; }}

    .title {{
      font-weight: 700;
      color: #203246;
      margin-bottom: 4px;
    }}

    .meta {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 3px;
    }}

    .mono {{
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      white-space: nowrap;
    }}

    a {{
      color: var(--blue);
      text-decoration: none;
    }}

    a:hover {{
      text-decoration: underline;
    }}

    .footer-note {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 12px;
    }}

    @media print {{
      body {{
        background: #fff;
      }}

      .page {{
        width: auto;
        margin: 0;
        border: 0;
        box-shadow: none;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    {body}
  </div>
</body>
</html>
"""


def render_keyword_section(keyword: str, rows: Sequence[dict], include_title: bool = True) -> str:
    slug = table_slug(keyword)
    heading = ""
    if include_title:
        heading = f"""
        <div class="section-title" id="{html.escape(slug)}">
          <h2>{html.escape(keyword)}</h2>
          <div class="count">{len(rows)} presentations</div>
        </div>
        """
    table_html = f"""
    <table>
      <thead>
        <tr>
          <th>Abstract #</th>
          <th>Authors</th>
          <th>Affiliation</th>
          <th>Title / Session</th>
          <th>Products</th>
        </tr>
      </thead>
      <tbody>
        {render_table_rows(rows)}
      </tbody>
    </table>
    """
    return heading + table_html


def render_summary_page(summary_rows: Sequence[dict]) -> str:
    summary_items = []
    for item in summary_rows:
        slug = table_slug(item["keyword"])
        summary_items.append(
            (
                f"<div class=\"summary-item\">"
                f"Displaying results 1 - 10 of <strong>{item['count']}</strong> for "
                f"<a href=\"combined_keyword_tables.html#{slug}\">{html.escape(item['keyword'])}</a>"
                f"</div>"
            )
        )
    body = f"""
    <div class="hero">
      <div class="eyebrow">AACR 2026</div>
      <h1>Poster Keyword Scan</h1>
    </div>
    <div class="content">
      <div class="summary-grid">
        {''.join(summary_items)}
      </div>
      <div class="section-title">
        <h2>Derived Views</h2>
      </div>
      <div class="summary-grid">
        <div class="summary-item">
          <a href="cross_platform.html">Cross Platform</a> flags presentations that mention more than one tracked product keyword,
          excluding 10X and Bruker.
        </div>
        <div class="summary-item">
          <a href="combined_keyword_tables.html">Combined keyword tables</a> keep every tracked cohort on one page for copy-paste review.
        </div>
        <div class="summary-item">
          <a href="insights.html">Insights dashboard</a> summarizes overlaps, institutions, and geography.
        </div>
      </div>
      <div class="footer-note">
        Counts are based on keyword searches run against the AACR Annual Meeting 2026 planner on {time.strftime('%Y-%m-%d')}.
      </div>
    </div>
    """
    return render_page("AACR 2026 Poster Keyword Summary", body)


def render_combined_page(summary_rows: Sequence[dict], keyword_rows: Dict[str, List[dict]]) -> str:
    summary_items = []
    for item in summary_rows:
        slug = table_slug(item["keyword"])
        summary_items.append(
            (
                f"<div class=\"summary-item\">"
                f"Displaying results 1 - 10 of <strong>{item['count']}</strong> for "
                f"<a href=\"#{slug}\">{html.escape(item['keyword'])}</a>"
                f"</div>"
            )
        )
    sections = [render_keyword_section(keyword, keyword_rows[keyword]) for keyword in keyword_rows]
    body = f"""
    <div class="hero">
      <div class="eyebrow">AACR 2026</div>
      <h1>Poster Keyword Tables</h1>
    </div>
    <div class="content">
      <div class="summary-grid">
        {''.join(summary_items)}
      </div>
      {''.join(sections)}
      <div class="footer-note">
        Derived views:
        <a href="cross_platform.html">Cross Platform</a> |
        <a href="insights.html">Insights Dashboard</a><br>
        Table columns follow the same structure as the reference flyer: abstract number, authors, affiliation, title, and detected products.
      </div>
    </div>
    """
    return render_page("AACR 2026 Poster Keyword Tables", body)


def render_single_keyword_page(keyword: str, rows: Sequence[dict]) -> str:
    body = f"""
    <div class="hero">
      <div class="eyebrow">AACR 2026</div>
      <h1>{html.escape(keyword)} Presentations</h1>
    </div>
    <div class="content">
      {render_keyword_section(keyword, rows, include_title=False)}
      <div class="footer-note">
        {len(rows)} presentations matched the keyword {html.escape(keyword)}.
      </div>
    </div>
    """
    return render_page(f"{keyword} Presentations", body)


def compute_cross_platform_rows(unique_rows: Sequence[dict]) -> List[dict]:
    rows = [row for row in unique_rows if len(set(row["products"])) > 1]
    return sorted(
        rows,
        key=lambda row: (-len(set(row["products"])), natural_abstract_sort_key(row["presentation_number"])),
    )


def render_cross_platform_page(rows: Sequence[dict]) -> str:
    body = f"""
    <div class="hero">
      <div class="eyebrow">AACR 2026</div>
      <h1>Cross Platform</h1>
    </div>
    <div class="content">
      <div class="footer-note">
        {len(rows)} presentations mention more than one tracked product keyword. 10X and Bruker are excluded from this derived list.
      </div>
      {render_keyword_section("Cross Platform", rows, include_title=False)}
      <div class="footer-note">
        Products are derived from tracked platform names only, so this page is a quick shortlist of abstracts that bridge multiple product families.
      </div>
    </div>
    """
    return render_page("Cross Platform", body)


def build_keyword_specs(values: Optional[Sequence[str]]) -> List[KeywordSpec]:
    if not values:
        return [KeywordSpec(label=label, query=query) for label, query in DEFAULT_KEYWORDS]

    specs: List[KeywordSpec] = []
    for value in values:
        if "=" in value:
            label, query = value.split("=", 1)
        else:
            label = value
            query = value
        specs.append(KeywordSpec(label=clean_text(label), query=clean_text(query)))
    return specs


def compute_overlap_pairs(keyword_rows: Dict[str, List[dict]]) -> Tuple[List[str], Dict[str, set], List[dict]]:
    labels = list(keyword_rows.keys())
    membership = {label: {row["presentation_id"] for row in rows} for label, rows in keyword_rows.items()}
    pairs: List[dict] = []
    for index, left in enumerate(labels):
        for right in labels[index + 1 :]:
            intersection = membership[left] & membership[right]
            union = membership[left] | membership[right]
            if not intersection:
                continue
            pairs.append(
                {
                    "left": left,
                    "right": right,
                    "shared_presentations": len(intersection),
                    "jaccard_pct": round((len(intersection) / len(union)) * 100, 1),
                }
            )
    pairs.sort(key=lambda row: (-row["shared_presentations"], row["left"], row["right"]))
    return labels, membership, pairs


def compute_affiliation_insights(unique_rows: Sequence[dict]) -> Tuple[List[dict], List[dict], List[dict]]:
    institution_counter: Counter = Counter()
    country_counter: Counter = Counter()
    state_counter: Counter = Counter()

    for row in unique_rows:
        seen_segments = set(split_affiliation_segments(row["affiliation"]))
        for segment in seen_segments:
            institution = extract_institution(segment)
            if institution:
                institution_counter[institution] += 1

            country_name, country_code, state_code, state_name = extract_geography(segment)
            if country_name and country_code:
                country_counter[(country_name, country_code)] += 1
            if state_code and state_name:
                state_counter[(state_code, state_name)] += 1

    institution_rows = [
        {"institution": institution, "presentation_count": count}
        for institution, count in institution_counter.most_common(15)
    ]
    country_rows = [
        {"country": country, "iso3": iso3, "presentation_count": count}
        for (country, iso3), count in country_counter.most_common()
    ]
    state_rows = [
        {"state_code": code, "state_name": name, "presentation_count": count}
        for (code, name), count in state_counter.most_common()
    ]
    return institution_rows, country_rows, state_rows


def compute_keyword_geography(keyword_rows: Dict[str, List[dict]], summary_rows: Sequence[dict]) -> Dict[str, dict]:
    counts_by_keyword = {row["keyword"]: int(row["count"]) for row in summary_rows}
    output: Dict[str, dict] = OrderedDict()

    for keyword, rows in keyword_rows.items():
        country_counter: Counter = Counter()
        state_counter: Counter = Counter()

        for row in rows:
            seen_segments = set(split_affiliation_segments(row["affiliation"]))
            for segment in seen_segments:
                country_name, country_code, state_code, state_name = extract_geography(segment)
                if country_name and country_code:
                    country_counter[(country_name, country_code)] += 1
                if state_code and state_name:
                    state_counter[(state_code, state_name)] += 1

        output[keyword] = {
            "count": counts_by_keyword.get(keyword, len(rows)),
            "countries": [
                {"country": country, "iso3": iso3, "presentation_count": count}
                for (country, iso3), count in country_counter.most_common()
            ],
            "states": [
                {"state_code": code, "state_name": name, "presentation_count": count}
                for (code, name), count in state_counter.most_common()
            ],
        }

    return output


def compute_product_rows(unique_rows: Sequence[dict]) -> List[dict]:
    counter: Counter = Counter()
    for row in unique_rows:
        for product in set(row["products"]):
            counter[product] += 1
    return [{"product": product, "presentation_count": count} for product, count in counter.most_common()]


def compute_day_rows(unique_rows: Sequence[dict]) -> List[dict]:
    counter: Counter = Counter()
    for row in unique_rows:
        if not row["start"]:
            continue
        try:
            dt = datetime.strptime(row["start"], "%m/%d/%Y %I:%M:%S %p")
        except ValueError:
            continue
        label = dt.strftime("%b %d").replace(" 0", " ")
        counter[label] += 1
    ordered = []
    for key in sorted(counter.keys(), key=lambda value: datetime.strptime(value + " 2026", "%b %d %Y")):
        ordered.append({"day": key, "presentation_count": counter[key]})
    return ordered


def color_for_ratio(ratio: float) -> str:
    blue = 255 - int(85 * ratio)
    red = 245 - int(75 * ratio)
    green = 249 - int(95 * ratio)
    return f"rgb({red}, {green}, {blue})"


def render_ranked_bars(rows: Sequence[dict], label_key: str, value_key: str) -> str:
    if not rows:
        return "<p>No data.</p>"
    max_value = max(int(row[value_key]) for row in rows) or 1
    blocks = []
    for row in rows:
        value = int(row[value_key])
        width = max(8, round((value / max_value) * 100))
        blocks.append(
            f"""
            <div class="bar-row">
              <div class="bar-label">{html.escape(str(row[label_key]))}</div>
              <div class="bar-track"><div class="bar-fill" style="width:{width}%"></div></div>
              <div class="bar-value">{value}</div>
            </div>
            """
        )
    return "".join(blocks)


def render_overlap_table(labels: Sequence[str], membership: Dict[str, set]) -> str:
    max_shared = max((len(membership[left] & membership[right]) for left in labels for right in labels), default=1)
    header = "".join(f"<th>{html.escape(label)}</th>" for label in labels)
    rows_html = []
    for left in labels:
        cells = [f"<th>{html.escape(left)}</th>"]
        for right in labels:
            shared = len(membership[left] & membership[right])
            ratio = shared / max_shared if max_shared else 0
            cells.append(
                f"<td style=\"background:{color_for_ratio(ratio)}\">{shared}</td>"
            )
        rows_html.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table class=\"heatmap\"><thead><tr><th></th>{header}</tr></thead><tbody>{''.join(rows_html)}</tbody></table>"


def render_insights_page(
    summary_rows: Sequence[dict],
    unique_rows: Sequence[dict],
    labels: Sequence[str],
    membership: Dict[str, set],
    overlap_pairs: Sequence[dict],
    institution_rows: Sequence[dict],
    country_rows: Sequence[dict],
    state_rows: Sequence[dict],
    keyword_geography: Dict[str, dict],
    product_rows: Sequence[dict],
    day_rows: Sequence[dict],
) -> str:
    total_hits = sum(int(row["count"]) for row in summary_rows)
    cards = [
        ("Keyword cohorts", len(summary_rows)),
        ("Keyword hits", total_hits),
        ("Unique presentations", len(unique_rows)),
        ("Xenium + 5K", next((row["count"] for row in summary_rows if row["keyword"] == "Xenium + 5K"), 0)),
    ]
    card_html = "".join(
        f"<div class=\"metric-card\"><div class=\"metric-value\">{value}</div><div class=\"metric-label\">{html.escape(label)}</div></div>"
        for label, value in cards
    )

    overlap_list = "".join(
        f"<tr><td>{html.escape(row['left'])}</td><td>{html.escape(row['right'])}</td><td>{row['shared_presentations']}</td><td>{row['jaccard_pct']}%</td></tr>"
        for row in overlap_pairs[:10]
    )

    country_map_rows = [row for row in country_rows if row["iso3"]]
    state_map_rows = [row for row in state_rows if row["state_code"]]
    keyword_geo_rows = {
        keyword: {
            "count": payload["count"],
            "countries": [row for row in payload["countries"] if row["iso3"]],
            "states": [row for row in payload["states"] if row["state_code"]],
        }
        for keyword, payload in keyword_geography.items()
    }

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AACR 2026 Keyword Insights</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --blue: #1673c6;
      --blue-dark: #0f4d8b;
      --ink: #1f2d3d;
      --muted: #60748a;
      --panel: #ffffff;
      --line: #d5e1eb;
      --soft: #f5fbff;
      --accent: #4fae6a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Aptos", "Segoe UI", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(22,115,198,0.12), transparent 28%),
        linear-gradient(180deg, #f5fbff 0%, #ffffff 22%);
    }}
    .page {{
      width: min(1700px, calc(100vw - 40px));
      margin: 24px auto 40px;
    }}
    .hero {{
      background: linear-gradient(135deg, #1d7bd0 0%, #0f5a9f 100%);
      color: #fff;
      padding: 28px 34px;
      border-radius: 18px;
      box-shadow: 0 20px 50px rgba(15, 77, 139, 0.12);
    }}
    .hero h1 {{
      margin: 8px 0 0;
      font-size: 38px;
      line-height: 1.05;
    }}
    .eyebrow {{
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 13px;
      opacity: 0.92;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin: 18px 0 24px;
    }}
    .metric-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px 20px;
      box-shadow: 0 16px 32px rgba(15, 77, 139, 0.06);
    }}
    .metric-value {{
      font-size: 34px;
      font-weight: 700;
      color: var(--blue-dark);
    }}
    .metric-label {{
      margin-top: 6px;
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 20px 22px;
      box-shadow: 0 16px 32px rgba(15, 77, 139, 0.06);
    }}
    .panel-wide {{
      grid-column: 1 / -1;
    }}
    .panel h2 {{
      margin: 0 0 12px;
      font-size: 24px;
      color: var(--blue-dark);
    }}
    .panel p {{
      margin: 0 0 14px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.45;
    }}
    .bar-row {{
      display: grid;
      grid-template-columns: 190px 1fr 52px;
      gap: 10px;
      align-items: center;
      margin-bottom: 10px;
    }}
    .bar-label {{
      font-size: 13px;
      color: var(--ink);
    }}
    .bar-track {{
      height: 12px;
      border-radius: 999px;
      background: #eaf2f8;
      overflow: hidden;
    }}
    .bar-fill {{
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, #4fae6a 0%, #1673c6 100%);
    }}
    .bar-value {{
      text-align: right;
      font-size: 13px;
      color: var(--muted);
      font-variant-numeric: tabular-nums;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      padding: 8px 9px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--blue-dark);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-size: 12px;
    }}
    .heatmap td {{
      text-align: center;
      font-variant-numeric: tabular-nums;
      min-width: 48px;
    }}
    .map {{
      height: 360px;
    }}
    .map-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }}
    .keyword-map-controls {{
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }}
    .keyword-map-controls label {{
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .keyword-map-controls select {{
      min-width: 240px;
      padding: 8px 10px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      font: inherit;
    }}
    .mini-note {{
      font-size: 13px;
      color: var(--muted);
    }}
    .note {{
      margin-top: 14px;
      font-size: 12px;
      color: var(--muted);
    }}
    @media (max-width: 1100px) {{
      .metrics, .grid {{ grid-template-columns: 1fr; }}
      .bar-row {{ grid-template-columns: 140px 1fr 44px; }}
      .map-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <div class="eyebrow">AACR 2026</div>
      <h1>Keyword Insights Dashboard</h1>
    </div>

    <div class="metrics">{card_html}</div>

    <div class="grid">
      <section class="panel">
        <h2>Keyword Volume</h2>
        <p>Counts below follow the exact planner queries used for the slide tables, including the compound Xenium + 5K search.</p>
        {render_ranked_bars(summary_rows, "keyword", "count")}
      </section>

      <section class="panel">
        <h2>Day Distribution</h2>
        <p>Unique presentations are concentrated across the main poster days, which helps frame when platform-specific activity peaks.</p>
        {render_ranked_bars(day_rows, "day", "presentation_count")}
      </section>

      <section class="panel">
        <h2>Keyword Overlap</h2>
        <p>Large shared counts show where cohorts are effectively nested or strongly co-mentioned. Xenium + 5K should read as a subset lens rather than a separate platform family.</p>
        {render_overlap_table(labels, membership)}
      </section>

      <section class="panel">
        <h2>Top Overlap Pairs</h2>
        <p>These are the strongest cross-cohort relationships by shared unique presentations.</p>
        <table>
          <thead>
            <tr><th>Left</th><th>Right</th><th>Shared</th><th>Jaccard</th></tr>
          </thead>
          <tbody>{overlap_list}</tbody>
        </table>
      </section>

      <section class="panel">
        <h2>Top Institutions</h2>
        <p>Institution names are derived from free-text affiliations, so this is a normalized heuristic rather than a cleaned master roster.</p>
        {render_ranked_bars(institution_rows[:12], "institution", "presentation_count")}
      </section>

      <section class="panel">
        <h2>Product Footprint</h2>
        <p>Products exclude umbrella search terms Bruker and 10X, per your request, so the view emphasizes actual platform names.</p>
        {render_ranked_bars(product_rows[:12], "product", "presentation_count")}
      </section>

      <section class="panel">
        <h2>Affiliation Countries</h2>
        <p>Counts use unique presentation-affiliation pairs. This gives a quick geographic sense of where the platform-related work is coming from.</p>
        <div id="country-map" class="map"></div>
        <div class="note">Country map coverage depends on recognizable country labels in the raw affiliation text.</div>
      </section>

      <section class="panel">
        <h2>US Affiliation States</h2>
        <p>US state density is useful for spotting domestic concentration, especially around major cancer centers and coastal biotech clusters.</p>
        <div id="state-map" class="map"></div>
      </section>

      <section class="panel panel-wide">
        <h2>Geography by Keyword</h2>
        <p>Select a keyword cohort to redraw both maps. This makes it easier to compare where each platform family is concentrated without losing the overall map context above.</p>
        <div class="keyword-map-controls">
          <label for="keyword-select">Keyword</label>
          <select id="keyword-select">
            {''.join(f'<option value="{html.escape(label)}">{html.escape(label)}</option>' for label in labels)}
          </select>
          <div id="keyword-map-meta" class="mini-note"></div>
        </div>
        <div class="map-grid">
          <div id="keyword-country-map" class="map"></div>
          <div id="keyword-state-map" class="map"></div>
        </div>
      </section>
    </div>
  </div>

  <script>
    const countryRows = {json.dumps(country_map_rows)};
    const stateRows = {json.dumps(state_map_rows)};
    const keywordGeo = {json.dumps(keyword_geo_rows)};

    function renderCountryMap(targetId, rows) {{
      Plotly.newPlot(targetId, [{{
        type: 'choropleth',
        locationmode: 'ISO-3',
        locations: rows.map(row => row.iso3),
        z: rows.map(row => row.presentation_count),
        text: rows.map(row => `${{row.country}}: ${{row.presentation_count}}`),
        colorscale: 'Blues',
        marker: {{line: {{color: '#ffffff', width: 0.4}}}},
        colorbar: {{title: 'Affiliations'}}
      }}], {{
        margin: {{t: 0, r: 0, b: 0, l: 0}},
        geo: {{projection: {{type: 'natural earth'}}, showframe: false, showcoastlines: false}}
      }}, {{displayModeBar: false, responsive: true}});
    }}

    function renderStateMap(targetId, rows) {{
      Plotly.newPlot(targetId, [{{
        type: 'choropleth',
        locationmode: 'USA-states',
        locations: rows.map(row => row.state_code),
        z: rows.map(row => row.presentation_count),
        text: rows.map(row => `${{row.state_name}}: ${{row.presentation_count}}`),
        colorscale: 'Blues',
        marker: {{line: {{color: '#ffffff', width: 0.4}}}},
        colorbar: {{title: 'Affiliations'}}
      }}], {{
        margin: {{t: 0, r: 0, b: 0, l: 0}},
        geo: {{scope: 'usa', showlakes: false, showframe: false}}
      }}, {{displayModeBar: false, responsive: true}});
    }}

    function updateKeywordMaps(keyword) {{
      const payload = keywordGeo[keyword] || {{count: 0, countries: [], states: []}};
      document.getElementById('keyword-map-meta').textContent =
        `${{payload.count}} presentations in cohort | ${{payload.countries.length}} country bins | ${{payload.states.length}} US state bins`;
      renderCountryMap('keyword-country-map', payload.countries);
      renderStateMap('keyword-state-map', payload.states);
    }}

    renderCountryMap('country-map', countryRows);
    renderStateMap('state-map', stateRows);
    const keywordSelect = document.getElementById('keyword-select');
    keywordSelect.addEventListener('change', event => updateKeywordMaps(event.target.value));
    updateKeywordMaps(keywordSelect.value);
  </script>
</body>
</html>"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape AACR abstract keyword hits into JSON, CSV, and HTML.")
    parser.add_argument("--meeting-id", default=MEETING_ID)
    parser.add_argument("--out-dir", default="output")
    parser.add_argument(
        "--keyword",
        action="append",
        dest="keywords",
        help="Keyword to search. Use LABEL=QUERY to override display text.",
    )
    args = parser.parse_args()

    keywords = build_keyword_specs(args.keywords)
    out_dir = Path(args.out_dir).resolve()
    data_dir = out_dir / "data"
    html_dir = out_dir / "html"
    keyword_html_dir = html_dir / "keywords"

    client = OasisClient(meeting_id=args.meeting_id)
    backpack_id = client.create_backpack()
    print(f"Created backpack {backpack_id}", file=sys.stderr)

    search_summary_rows: List[dict] = []
    keyword_hits: Dict[str, List[dict]] = OrderedDict()
    presentation_to_keywords: Dict[str, List[str]] = defaultdict(list)

    for keyword in keywords:
        search = client.search_presentations(keyword)
        results = client.fetch_search_results(search["SearchId"], keyword.query)
        expected_count = int(search.get("Count") or 0)
        if expected_count != len(results):
            raise RuntimeError(
                f"{keyword.label}: expected {expected_count} results, fetched {len(results)}"
            )

        keyword_hits[keyword.label] = results
        search_summary_rows.append(
            {
                "keyword": keyword.label,
                "query": keyword.query,
                "count": expected_count,
                "search_id": clean_text(search.get("SearchId")),
                "status": clean_text(search.get("Status")),
            }
        )
        print(f"{keyword.label}: {expected_count} results", file=sys.stderr)

        for result in results:
            presentation_id = clean_text(result.get("Id"))
            if keyword.label not in presentation_to_keywords[presentation_id]:
                presentation_to_keywords[presentation_id].append(keyword.label)

    details_by_id: Dict[str, dict] = {}
    unique_ids = sorted(presentation_to_keywords.keys())
    for index, presentation_id in enumerate(unique_ids, start=1):
        query_context = presentation_to_keywords[presentation_id][0]
        details_by_id[presentation_id] = client.fetch_presentation(presentation_id, query_context)
        if index % 25 == 0 or index == len(unique_ids):
            print(f"Fetched {index}/{len(unique_ids)} presentation details", file=sys.stderr)
        time.sleep(0.03)

    normalized_by_id: Dict[str, dict] = {}
    for presentation_id, detail in details_by_id.items():
        normalized_by_id[presentation_id] = normalize_presentation(
            detail,
            source_keywords=presentation_to_keywords[presentation_id],
        )

    keyword_rows: Dict[str, List[dict]] = OrderedDict()
    hit_rows: List[dict] = []
    for keyword in keywords:
        rows: List[dict] = []
        for result in keyword_hits[keyword.label]:
            record = dict(normalized_by_id[clean_text(result.get("Id"))])
            rows.append(record)
            hit_rows.append(
                {
                    "keyword": keyword.label,
                    "presentation_id": record["presentation_id"],
                    "presentation_number": record["presentation_number"],
                    "title": record["title"],
                    "session_title": record["session_title"],
                    "start": record["start"],
                    "authors": record["authors"],
                    "affiliation": record["affiliation"],
                    "products": csv_join(record["products"]),
                    "source_keywords": csv_join(record["source_keywords"]),
                    "presentation_url": record["presentation_url"],
                }
            )
        keyword_rows[keyword.label] = sorted(rows, key=lambda row: natural_abstract_sort_key(row["presentation_number"]))

    unique_rows = sorted(
        normalized_by_id.values(),
        key=lambda row: natural_abstract_sort_key(row["presentation_number"]),
    )
    overlap_labels, overlap_membership, overlap_pairs = compute_overlap_pairs(keyword_rows)
    institution_rows, country_rows, state_rows = compute_affiliation_insights(unique_rows)
    keyword_geography = compute_keyword_geography(keyword_rows, search_summary_rows)
    product_rows = compute_product_rows(unique_rows)
    day_rows = compute_day_rows(unique_rows)
    cross_platform_rows = compute_cross_platform_rows(unique_rows)

    write_json(data_dir / "keyword_summary.json", search_summary_rows)
    write_json(data_dir / "presentations_unique.json", unique_rows)
    write_json(data_dir / "presentations_by_keyword.json", keyword_rows)
    write_json(data_dir / "keyword_overlap_pairs.json", overlap_pairs)
    write_json(data_dir / "institution_counts.json", institution_rows)
    write_json(data_dir / "country_counts.json", country_rows)
    write_json(data_dir / "us_state_counts.json", state_rows)
    write_json(data_dir / "keyword_geography.json", keyword_geography)
    write_json(data_dir / "product_counts.json", product_rows)
    write_json(data_dir / "day_counts.json", day_rows)
    write_json(data_dir / "cross_platform.json", cross_platform_rows)

    write_csv(
        data_dir / "keyword_summary.csv",
        search_summary_rows,
        ["keyword", "query", "count", "search_id", "status"],
    )
    write_csv(
        data_dir / "presentation_hits_by_keyword.csv",
        hit_rows,
        [
            "keyword",
            "presentation_id",
            "presentation_number",
            "title",
            "session_title",
            "start",
            "authors",
            "affiliation",
            "products",
            "source_keywords",
            "presentation_url",
        ],
    )
    write_csv(
        data_dir / "presentations_unique.csv",
        [
            {
                **row,
                "products": csv_join(row["products"]),
                "source_keywords": csv_join(row["source_keywords"]),
            }
            for row in unique_rows
        ],
        [
            "presentation_id",
            "presentation_number",
            "title",
            "session_title",
            "session_id",
            "start",
            "end",
            "posterboard_number",
            "authors",
            "affiliation",
            "abstract",
            "disclosures",
            "presenter",
            "activity",
            "status",
            "keywords_field",
            "topics_field",
            "products",
            "source_keywords",
            "presentation_url",
        ],
    )
    write_csv(
        data_dir / "keyword_overlap_pairs.csv",
        overlap_pairs,
        ["left", "right", "shared_presentations", "jaccard_pct"],
    )
    write_csv(
        data_dir / "institution_counts.csv",
        institution_rows,
        ["institution", "presentation_count"],
    )
    write_csv(
        data_dir / "country_counts.csv",
        country_rows,
        ["country", "iso3", "presentation_count"],
    )
    write_csv(
        data_dir / "us_state_counts.csv",
        state_rows,
        ["state_code", "state_name", "presentation_count"],
    )
    write_csv(
        data_dir / "keyword_country_counts.csv",
        [
            {"keyword": keyword, **row}
            for keyword, payload in keyword_geography.items()
            for row in payload["countries"]
        ],
        ["keyword", "country", "iso3", "presentation_count"],
    )
    write_csv(
        data_dir / "keyword_us_state_counts.csv",
        [
            {"keyword": keyword, **row}
            for keyword, payload in keyword_geography.items()
            for row in payload["states"]
        ],
        ["keyword", "state_code", "state_name", "presentation_count"],
    )
    write_csv(
        data_dir / "product_counts.csv",
        product_rows,
        ["product", "presentation_count"],
    )
    write_csv(
        data_dir / "day_counts.csv",
        day_rows,
        ["day", "presentation_count"],
    )
    write_csv(
        data_dir / "cross_platform.csv",
        [
            {
                **row,
                "products": csv_join(row["products"]),
                "source_keywords": csv_join(row["source_keywords"]),
            }
            for row in cross_platform_rows
        ],
        [
            "presentation_id",
            "presentation_number",
            "title",
            "session_title",
            "session_id",
            "start",
            "end",
            "posterboard_number",
            "authors",
            "affiliation",
            "abstract",
            "disclosures",
            "presenter",
            "activity",
            "status",
            "keywords_field",
            "topics_field",
            "products",
            "source_keywords",
            "presentation_url",
        ],
    )

    summary_html = render_summary_page(search_summary_rows)
    (html_dir / "index.html").parent.mkdir(parents=True, exist_ok=True)
    (html_dir / "index.html").write_text(summary_html, encoding="utf-8")
    (html_dir / "combined_keyword_tables.html").write_text(
        render_combined_page(search_summary_rows, keyword_rows),
        encoding="utf-8",
    )
    (html_dir / "insights.html").write_text(
        render_insights_page(
            search_summary_rows,
            unique_rows,
            overlap_labels,
            overlap_membership,
            overlap_pairs,
            institution_rows,
            country_rows,
            state_rows,
            keyword_geography,
            product_rows,
            day_rows,
        ),
        encoding="utf-8",
    )
    (html_dir / "cross_platform.html").write_text(
        render_cross_platform_page(cross_platform_rows),
        encoding="utf-8",
    )

    keyword_html_dir.mkdir(parents=True, exist_ok=True)
    expected_keyword_files = {f"{table_slug(keyword)}.html" for keyword in keyword_rows}
    for stale_path in keyword_html_dir.glob("*.html"):
        if stale_path.name not in expected_keyword_files:
            stale_path.unlink()
    for keyword, rows in keyword_rows.items():
        (keyword_html_dir / f"{table_slug(keyword)}.html").write_text(
            render_single_keyword_page(keyword, rows),
            encoding="utf-8",
        )

    print(f"Wrote outputs to {out_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
