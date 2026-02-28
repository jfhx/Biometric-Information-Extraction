"""
Utilities for post-processing LLM extraction results:
1. Split date strings (YYYY-MM-DD) into separate year/month/day fields.
2. Standardize country / province names against dict_country_global_all.xlsx.
3. Standardize pathogen names against dict_pathogen_feature.xlsx.
4. Standardize host names against dict_host_tag.xlsx.
5. Track unmatched names.
"""

import re
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Date splitting
# ---------------------------------------------------------------------------

def split_date(date_str: str) -> Tuple[str, str, str]:
    """Split a date string like '2026-01-16' into ('2026', '01', '16').

    Handles partial dates:
      '2025-12'   -> ('2025', '12', '')
      '2025'      -> ('2025', '', '')
      ''          -> ('', '', '')
    """
    if not date_str or not date_str.strip():
        return ("", "", "")
    cleaned = date_str.strip()
    parts = cleaned.split("-")
    year = parts[0] if len(parts) >= 1 else ""
    month = parts[1] if len(parts) >= 2 else ""
    day = parts[2] if len(parts) >= 3 else ""
    return (year.strip(), month.strip(), day.strip())


# ---------------------------------------------------------------------------
# Country / Province standardization
# ---------------------------------------------------------------------------

class CountryProvinceStandardizer:
    """Load reference table and provide fast lookup for country & province."""

    def __init__(self, xlsx_path: str | Path):
        self._lock = threading.Lock()
        self._unmatched_countries: Set[str] = set()
        self._unmatched_provinces: Set[str] = set()

        df = pd.read_excel(xlsx_path)
        # Build lookup maps (all keys lower-cased for case-insensitive match)
        # country_map: {lower_name: standard_country, ...}
        self._country_map: Dict[str, str] = {}
        # province_map: {(lower_country, lower_province): standard_province}
        self._province_map: Dict[Tuple[str, str], str] = {}
        # province_by_name_only: {lower_province: [(std_country, std_province), ...]}
        self._province_by_name: Dict[str, List[Tuple[str, str]]] = {}

        for _, row in df.iterrows():
            country = str(row.get("country", "")).strip()
            full_name = str(row.get("country_full_name", "")).strip()
            province = str(row.get("province", "")).strip()

            if country:
                self._country_map[country.lower()] = country
            if full_name:
                self._country_map[full_name.lower()] = country

            if country and province:
                key = (country.lower(), province.lower())
                self._province_map[key] = province
                self._province_by_name.setdefault(province.lower(), []).append(
                    (country, province)
                )

        # Build a set of all standard country names (lower) for substring matching
        self._all_countries_lower: Dict[str, str] = dict(self._country_map)

    def standardize_country(self, raw_country: str) -> str:
        """Return the standard country name, or the original if not found."""
        if not raw_country or not raw_country.strip():
            return ""
        cleaned = raw_country.strip()
        key = cleaned.lower()

        # 1) Exact match (case-insensitive)
        if key in self._country_map:
            return self._country_map[key]

        # 2) Try stripping common prefixes/suffixes like "The ", "Republic of "
        normalized = re.sub(
            r"^(the\s+|republic\s+of\s+)", "", key, flags=re.IGNORECASE
        ).strip()
        if normalized in self._country_map:
            return self._country_map[normalized]

        # 3) Substring containment: prefer the longest (most specific) match
        best_match = ""
        best_match_len = 0
        for std_lower, std_name in self._all_countries_lower.items():
            if std_lower in key or key in std_lower:
                if len(std_lower) > best_match_len:
                    best_match = std_name
                    best_match_len = len(std_lower)
        if best_match:
            return best_match

        # Not found — record as unmatched
        with self._lock:
            self._unmatched_countries.add(cleaned)
        return cleaned

    def standardize_province(
        self, raw_province: str, standardized_country: str
    ) -> str:
        """Return the standard province name within the given country."""
        if not raw_province or not raw_province.strip():
            return ""
        cleaned = raw_province.strip()
        country_key = standardized_country.strip().lower() if standardized_country else ""
        province_key = cleaned.lower()

        # 1) Exact match with country context
        if country_key:
            pair = (country_key, province_key)
            if pair in self._province_map:
                return self._province_map[pair]

        # 2) Try province name only (may match multiple countries)
        if province_key in self._province_by_name:
            matches = self._province_by_name[province_key]
            # If country context provided, prefer that
            if country_key:
                for std_c, std_p in matches:
                    if std_c.lower() == country_key:
                        return std_p
            # Otherwise return first match
            return matches[0][1]

        # 3) Substring matching within the country's provinces
        if country_key:
            for (ck, pk), std_p in self._province_map.items():
                if ck == country_key and (pk in province_key or province_key in pk):
                    return std_p

        # Not found
        with self._lock:
            self._unmatched_provinces.add(f"{standardized_country}|{cleaned}")
        return cleaned

    def get_unmatched_countries(self) -> List[str]:
        with self._lock:
            return sorted(self._unmatched_countries)

    def get_unmatched_provinces(self) -> List[str]:
        with self._lock:
            return sorted(self._unmatched_provinces)

    def save_unmatched(self, output_path: str | Path) -> None:
        """Save unmatched country/province names to a text file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        countries = self.get_unmatched_countries()
        provinces = self.get_unmatched_provinces()
        if not countries and not provinces:
            return
        with open(path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("Unmatched Country Names (not in dict_country_global_all)\n")
            f.write("=" * 60 + "\n")
            for c in countries:
                f.write(f"  {c}\n")
            f.write("\n")
            f.write("=" * 60 + "\n")
            f.write("Unmatched Province Names (country|province)\n")
            f.write("=" * 60 + "\n")
            for p in provinces:
                f.write(f"  {p}\n")
        print(f"Unmatched names saved to: {path}")


# ---------------------------------------------------------------------------
# Pathogen standardization
# ---------------------------------------------------------------------------

def _normalize_key(s: str) -> str:
    """Normalize a string for fuzzy matching: lower, replace hyphens/underscores/spaces."""
    return re.sub(r"[-_\s]+", "_", s.strip().lower())


class PathogenStandardizer:
    """Match LLM-extracted pathogen against dict_pathogen_feature.xlsx.

    Priority: pathogen (most specific) > pathogen_rank_2 > pathogen_rank_1.
    Also matches via pathogen_name (human-readable name).
    """

    def __init__(self, xlsx_path: str | Path):
        self._lock = threading.Lock()
        self._unmatched: Set[str] = set()

        df = pd.read_excel(xlsx_path)
        # pathogen_lookup: {normalized_key: (pathogen, rank1, rank2)}
        self._pathogen_lookup: Dict[str, Tuple[str, str, str]] = {}
        # Also index by pathogen_name for human-readable matching
        self._name_lookup: Dict[str, Tuple[str, str, str]] = {}
        # rank2 -> (pathogen kept empty, rank1, rank2)
        self._rank2_lookup: Dict[str, Tuple[str, str]] = {}
        # rank1 -> rank1
        self._rank1_set: Dict[str, str] = {}

        for _, row in df.iterrows():
            pathogen = str(row.get("pathogen", "")).strip()
            rank1 = str(row.get("pathogen_rank_1", "")).strip()
            rank2 = str(row.get("pathogen_rank_2", "")).strip()
            pname = str(row.get("pathogen_name", "")).strip()

            if pathogen:
                self._pathogen_lookup[_normalize_key(pathogen)] = (pathogen, rank1, rank2)
            if pname:
                self._name_lookup[_normalize_key(pname)] = (pathogen, rank1, rank2)
            if rank2:
                self._rank2_lookup.setdefault(_normalize_key(rank2), (rank1, rank2))
            if rank1:
                self._rank1_set.setdefault(_normalize_key(rank1), rank1)

    def standardize(self, raw_pathogen: str) -> Tuple[str, str, str]:
        """Return (pathogen, pathogen_rank_1, pathogen_rank_2)."""
        if not raw_pathogen or not raw_pathogen.strip():
            return ("", "", "")
        cleaned = raw_pathogen.strip()
        key = _normalize_key(cleaned)

        # 1) Exact match on pathogen code (normalized)
        if key in self._pathogen_lookup:
            return self._pathogen_lookup[key]

        # 2) Exact match on pathogen_name (normalized)
        if key in self._name_lookup:
            return self._name_lookup[key]

        # 3) Substring match on pathogen_name (e.g. "H5N1" in "Influenza A H5N1")
        best_name_match: Optional[Tuple[str, str, str]] = None
        best_name_len = 0
        for nm_key, triple in self._name_lookup.items():
            if key in nm_key or nm_key in key:
                if len(nm_key) > best_name_len:
                    best_name_match = triple
                    best_name_len = len(nm_key)
        if best_name_match:
            return best_name_match

        # 4) Substring match on pathogen code
        best_code_match: Optional[Tuple[str, str, str]] = None
        best_code_len = 0
        for code_key, triple in self._pathogen_lookup.items():
            if code_key in key or key in code_key:
                if len(code_key) > best_code_len:
                    best_code_match = triple
                    best_code_len = len(code_key)
        if best_code_match:
            return best_code_match

        # 5) Try matching as rank2
        if key in self._rank2_lookup:
            r1, r2 = self._rank2_lookup[key]
            return ("", r1, r2)

        # 6) Try matching as rank1
        if key in self._rank1_set:
            return ("", self._rank1_set[key], "")

        # Not found
        with self._lock:
            self._unmatched.add(cleaned)
        return (cleaned, "", "")

    def get_unmatched(self) -> List[str]:
        with self._lock:
            return sorted(self._unmatched)


# ---------------------------------------------------------------------------
# Host standardization
# ---------------------------------------------------------------------------

class HostStandardizer:
    """Match LLM-extracted host against dict_host_tag.xlsx.

    host keeps the original LLM value.
    host_rank_1 is the broad category (Human, Mammal, Avian, Arthropod, etc.).
    host_rank_2 is the more specific name (Dove, Mosquito, Pig, etc.).
    """

    def __init__(self, xlsx_path: str | Path):
        self._lock = threading.Lock()
        self._unmatched: Set[str] = set()

        df = pd.read_excel(xlsx_path)
        # host_lookup: {lower_host: (host_rank_1, host_rank_2)}
        self._host_lookup: Dict[str, Tuple[str, str]] = {}
        # rank2_lookup: {lower_rank2: (rank1, rank2)}
        self._rank2_lookup: Dict[str, Tuple[str, str]] = {}
        # rank1_set: {lower_rank1: rank1}
        self._rank1_set: Dict[str, str] = {}

        for _, row in df.iterrows():
            host = str(row.get("host", "")).strip()
            rank1 = str(row.get("host_rank_1", "")).strip()
            rank2 = str(row.get("host_rank_2", "")).strip()

            if host:
                self._host_lookup[host.lower()] = (rank1, rank2)
            if rank2:
                self._rank2_lookup.setdefault(rank2.lower(), (rank1, rank2))
            if rank1:
                self._rank1_set.setdefault(rank1.lower(), rank1)

    def standardize(self, raw_host: str) -> Tuple[str, str]:
        """Return (host_rank_1, host_rank_2). raw_host is kept as-is."""
        if not raw_host or not raw_host.strip():
            return ("", "")
        cleaned = raw_host.strip()
        key = cleaned.lower()

        # 1) Exact match on host
        if key in self._host_lookup:
            return self._host_lookup[key]

        # 2) Exact match on rank2 (e.g. LLM says "Dove")
        if key in self._rank2_lookup:
            return self._rank2_lookup[key]

        # 3) Exact match on rank1 (e.g. LLM says "Human")
        if key in self._rank1_set:
            return (self._rank1_set[key], "")

        # 4) Substring match on host names (prefer longest)
        best_match: Optional[Tuple[str, str]] = None
        best_len = 0
        for h_lower, pair in self._host_lookup.items():
            if h_lower in key or key in h_lower:
                if len(h_lower) > best_len:
                    best_match = pair
                    best_len = len(h_lower)
        if best_match:
            return best_match

        # 5) Substring match on rank2
        best_r2: Optional[Tuple[str, str]] = None
        best_r2_len = 0
        for r2_lower, pair in self._rank2_lookup.items():
            if r2_lower in key or key in r2_lower:
                if len(r2_lower) > best_r2_len:
                    best_r2 = pair
                    best_r2_len = len(r2_lower)
        if best_r2:
            return best_r2

        # Not found
        with self._lock:
            self._unmatched.add(cleaned)
        return ("", "")

    def get_unmatched(self) -> List[str]:
        with self._lock:
            return sorted(self._unmatched)


# ---------------------------------------------------------------------------
# Save all unmatched names
# ---------------------------------------------------------------------------

def save_all_unmatched(
    output_path: str | Path,
    country_std: Optional[CountryProvinceStandardizer] = None,
    pathogen_std: Optional[PathogenStandardizer] = None,
    host_std: Optional[HostStandardizer] = None,
) -> None:
    """Save all unmatched names from all standardizers to a single file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    sections: List[Tuple[str, List[str]]] = []
    if country_std:
        uc = country_std.get_unmatched_countries()
        up = country_std.get_unmatched_provinces()
        if uc:
            sections.append(("Unmatched Country Names (not in dict_country_global_all)", uc))
        if up:
            sections.append(("Unmatched Province Names (country|province)", up))
    if pathogen_std:
        upath = pathogen_std.get_unmatched()
        if upath:
            sections.append(("Unmatched Pathogen Names (not in dict_pathogen_feature)", upath))
    if host_std:
        uhost = host_std.get_unmatched()
        if uhost:
            sections.append(("Unmatched Host Names (not in dict_host_tag)", uhost))

    if not sections:
        return

    with open(path, "w", encoding="utf-8") as f:
        for title, items in sections:
            f.write("=" * 60 + "\n")
            f.write(title + "\n")
            f.write("=" * 60 + "\n")
            for item in items:
                f.write(f"  {item}\n")
            f.write("\n")
    print(f"Unmatched names saved to: {path}")


# ---------------------------------------------------------------------------
# Post-processing: enrich a record dict with date splits + standardization
# ---------------------------------------------------------------------------

def enrich_record(
    record: Dict[str, str],
    standardizer: Optional[CountryProvinceStandardizer] = None,
    pathogen_std: Optional[PathogenStandardizer] = None,
    host_std: Optional[HostStandardizer] = None,
) -> Dict[str, str]:
    """Add date-split fields and standardize country/province/pathogen/host in-place.

    Returns the same dict with extra keys inserted.
    """
    # --- Date splitting ---
    sy, sm, sd = split_date(record.get("start_date", ""))
    record["start_date_year"] = sy
    record["start_date_month"] = sm
    record["start_date_day"] = sd

    ey, em, ed = split_date(record.get("end_date", ""))
    record["end_date_year"] = ey
    record["end_date_month"] = em
    record["end_date_day"] = ed

    # --- Country / Province standardization ---
    if standardizer:
        for prefix in ("original", "spread"):
            country_field = f"{prefix}_country"
            province_field = f"{prefix}_province"
            raw_country = record.get(country_field, "")
            raw_province = record.get(province_field, "")
            std_country = standardizer.standardize_country(raw_country)
            std_province = standardizer.standardize_province(raw_province, std_country)
            record[country_field] = std_country
            record[province_field] = std_province

    # --- Pathogen standardization ---
    if pathogen_std:
        raw_pathogen = record.get("pathogen", "")
        std_p, std_r1, std_r2 = pathogen_std.standardize(raw_pathogen)
        record["pathogen"] = std_p
        record["pathogen_rank_1"] = std_r1
        record["pathogen_rank_2"] = std_r2
    else:
        record["pathogen_rank_1"] = ""
        record["pathogen_rank_2"] = ""

    # --- Host standardization ---
    if host_std:
        raw_host = record.get("host", "")
        h_r1, h_r2 = host_std.standardize(raw_host)
        record["host_rank_1"] = h_r1
        record["host_rank_2"] = h_r2
    else:
        record["host_rank_1"] = ""
        record["host_rank_2"] = ""

    return record
