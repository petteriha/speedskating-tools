# src/topn.py
from __future__ import annotations

import csv
import io
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
import xml.etree.ElementTree as ET

API_BASE = "https://speedskatingresults.com/api/xml/topn"
USER_AGENT = "Mozilla/5.0 (compatible; TopN-RangeBot/streamlit; +https://speedskatingresults.com)"
DEFAULT_TIMEOUT = 30

ALLOWED_DISTANCES = [300, 500, 1000, 1500, 3000, 5000, 10000]


@dataclass(frozen=True)
class ResultRow:
    age: str                 # esim FA2
    distance: int            # metres
    date: str
    event: str
    skater_name: str
    skater_id: str
    time_str: str            # alkuperäinen (esim "1.23,45")
    time_seconds: float      # sekunteina


def parse_ageclass(ageclass: str) -> Tuple[str, str]:
    """
    MB2 -> (gender='m', age='B2')
    FA2 -> (gender='f', age='A2')
    """
    s = (ageclass or "").strip()
    if not s or len(s) < 2:
        raise ValueError("Ageclass pitää olla esim. MB2 / FA2")
    g = s[0].upper()
    if g not in ("M", "F"):
        raise ValueError("Ageclass pitää alkaa M tai F, esim. MB2 tai FA2")
    gender = g.lower()
    age = s[1:].strip()
    if not age:
        raise ValueError("Ikäluokka puuttuu (esim. A2/B2/...)")
    return gender, age


def normalize_name(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def parse_time_to_seconds(t: str) -> Optional[float]:
    """
    Tukee:
      - "37,45"  -> 37.45
      - "37.45"  -> 37.45
      - "1:10,23" / "1:10.23" -> 70.23
      - "2.33,86" (SSR-tyyli) -> 2:33.86
      - "2.33.86" -> 2:33.86
    """
    if not t:
        return None

    s = t.strip().strip('"')
    if s in ("--", "-", ""):
        return None

    # DNS/DQ/etc.
    if re.search(r"[A-Za-z]", s):
        return None

    s = re.sub(r"\s+", "", s)

    # "m.ss,cc"
    m = re.fullmatch(r"(\d+)\.(\d{2}),(\d{2})", s)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        hundredths = int(m.group(3))
        return minutes * 60 + seconds + hundredths / 100.0

    # "m.ss.cc"
    m = re.fullmatch(r"(\d+)\.(\d{2})\.(\d{2})", s)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        hundredths = int(m.group(3))
        return minutes * 60 + seconds + hundredths / 100.0

    # "m:ss,cc" / "m:ss.cc"
    if ":" in s:
        s2 = s.replace(",", ".")
        parts = s2.split(":")
        if len(parts) != 2:
            return None
        if not re.fullmatch(r"\d+", parts[0]):
            return None
        if not re.fullmatch(r"\d+(\.\d+)?", parts[1]):
            return None
        return int(parts[0]) * 60 + float(parts[1])

    # "ss,cc" / "ss.cc"
    s2 = s.replace(",", ".")
    if re.fullmatch(r"\d+(\.\d+)?", s2):
        return float(s2)

    return None


def build_skater_name(skater_el: ET.Element) -> str:
    given = (skater_el.findtext("givenname") or "").strip()
    family = (skater_el.findtext("familyname") or "").strip()

    if not given and not family:
        given = (skater_el.findtext("givennative") or "").strip()
        family = (skater_el.findtext("familynative") or "").strip()

    name = " ".join([p for p in [given, family] if p]).strip()
    return name or "(unknown)"


def _requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def fetch_topn_xml(
    session: requests.Session,
    *,
    season: int,
    gender: str,
    distance: int,
    max_n: int,
    age: Optional[str],
    country: Optional[str],
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = 3,
    backoff_s: float = 1.0,
) -> ET.Element:
    params = {
        "season": str(season),
        "gender": gender,
        "distance": str(distance),
        "max": str(max_n),
    }
    if age:
        params["age"] = age
    if country:
        params["country"] = country

    url = f"{API_BASE}?{urlencode(params)}"

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return ET.fromstring(r.content)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s * attempt)
            else:
                raise RuntimeError(f"API-kutsu epäonnistui ({season=}, distance={distance}): {url}\n{e}") from e

    raise RuntimeError(f"API-kutsu epäonnistui: {url}\n{last_err}")


def extract_rows(root: ET.Element, *, ageclass: str, distance: int) -> List[ResultRow]:
    out: List[ResultRow] = []
    for res in root.findall(".//result"):
        time_str = (res.findtext("time") or "").strip()
        date = (res.findtext("date") or "").strip()
        event = (res.findtext("event") or "").strip()

        skater_el = res.find("skater")
        if skater_el is None:
            skater_id = ""
            skater_name = "(unknown)"
        else:
            skater_id = (skater_el.findtext("id") or "").strip()
            skater_name = build_skater_name(skater_el)

        secs = parse_time_to_seconds(time_str)
        if secs is None:
            continue

        out.append(
            ResultRow(
                age=ageclass,
                distance=distance,
                date=date,
                event=event,
                skater_name=skater_name,
                skater_id=skater_id,
                time_str=time_str,
                time_seconds=secs,
            )
        )
    return out


def run_topn_query(
    *,
    ageclass: str,
    distances: List[int],
    start_season: int,
    end_season: int,
    top_n: int,
    per_season_top: int = 5,
    country: str = "FIN",   # "world" => ei country-parametria
    timeout: int = DEFAULT_TIMEOUT,
    polite_sleep_s: float = 0.02,
) -> Tuple[List[Dict[str, object]], Dict[int, Dict[str, object]]]:
    """
    Palauttaa:
      - rows: lista rivejä (dict), sis. Rank per distance, ja järjestetty distance + time_seconds
      - summary: per distance debug-yhteenveto
    """
    if not distances:
        raise ValueError("distances tyhjä")
    for d in distances:
        if d not in ALLOWED_DISTANCES:
            raise ValueError(f"Ei sallittu matka: {d}")

    gender, age = parse_ageclass(ageclass)

    start = min(start_season, end_season)
    end = max(start_season, end_season)

    # country param:
    c_in = (country or "").strip()
    country_param: Optional[str]
    if c_in == "" or c_in.lower() == "world":
        country_param = None
    else:
        # pidä numerot sellaisenaan, kirjaimet isoiksi
        country_param = c_in.upper() if c_in.isalpha() else c_in

    session = _requests_session()

    per_season_fetch = max(per_season_top, min(300, top_n * 3))

    # Dedup: per (distance, skater) paras aika
    best_by_key: Dict[Tuple[int, str], ResultRow] = {}

    api_errors_by_distance: Dict[int, List[str]] = {d: [] for d in distances}
    fetched_rows_by_distance: Dict[int, int] = {d: 0 for d in distances}

    for distance in sorted(distances):
        for season in range(start, end + 1):
            try:
                root = fetch_topn_xml(
                    session,
                    season=season,
                    gender=gender,
                    distance=distance,
                    max_n=per_season_fetch,
                    age=age,
                    country=country_param,
                    timeout=timeout,
                )
            except Exception as e:
                api_errors_by_distance[distance].append(f"season={season}: {e}")
                continue

            rows = extract_rows(root, ageclass=ageclass, distance=distance)
            if not rows:
                continue

            fetched_rows_by_distance[distance] += len(rows)

            for row in rows:
                sk_key = row.skater_id.strip() or f"name:{normalize_name(row.skater_name)}"
                key = (distance, sk_key)
                prev = best_by_key.get(key)
                if prev is None or row.time_seconds < prev.time_seconds:
                    best_by_key[key] = row

            if polite_sleep_s:
                time.sleep(polite_sleep_s)

    # Tee TopN per distance + Rank per distance
    out_rows: List[Dict[str, object]] = []
    summary: Dict[int, Dict[str, object]] = {}

    for d in sorted(distances):
        d_rows = [r for (dist, _), r in best_by_key.items() if dist == d]
        d_rows.sort(key=lambda r: r.time_seconds)

        summary[d] = {
            "raw_rows": fetched_rows_by_distance.get(d, 0),
            "unique_skaters": len(d_rows),
            "written": min(top_n, len(d_rows)),
            "errors": api_errors_by_distance.get(d, []),
        }

        for idx, r in enumerate(d_rows[:top_n], start=1):
            out_rows.append(
                {
                    "Rank": idx,
                    "Age": r.age,
                    "Distance": r.distance,
                    "Date": r.date,
                    "Event": r.event,
                    "SkaterName": r.skater_name,
                    "SkaterId": r.skater_id,
                    "Time": r.time_str,
                    "TimeSeconds": round(float(r.time_seconds), 3),
                }
            )

    # Lopullinen järjestys: matka, aika (Rank jo on matkan sisäinen)
    out_rows.sort(key=lambda x: (int(x["Distance"]), float(x["TimeSeconds"])))
    return out_rows, summary


def to_csv_bytes(rows: List[Dict[str, object]]) -> bytes:
    """
    Muodostaa CSV:n bytesinä (UTF-8). Sopii Streamlit download_buttonille.
    """
    if not rows:
        header = ["Rank", "Age", "Distance", "Date", "Event", "SkaterName", "SkaterId", "Time", "TimeSeconds"]
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=header)
        w.writeheader()
        return buf.getvalue().encode("utf-8")

    fieldnames = ["Rank", "Age", "Distance", "Date", "Event", "SkaterName", "SkaterId", "Time", "TimeSeconds"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode("utf-8")
