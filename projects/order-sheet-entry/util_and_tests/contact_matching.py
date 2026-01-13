"""
contact_matching.py

Match customer names to contact IDs from database.
"""

import re
import logging
from pathlib import Path

import pandas as pd

from .csv_utils import read_csv_smart


def normalize_name(name: str) -> str:
    """Normalize name for matching: lowercase, strip punctuation, collapse spaces."""
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)  # remove punctuation
    name = re.sub(r"\s+", " ", name).strip()  # collapse spaces
    return name


def load_contacts(contacts_path: Path) -> pd.DataFrame:
    """Load contacts CSV and prepare for matching."""
    df = read_csv_smart(contacts_path, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    df["Name"] = df["Name"].fillna("").astype(str).str.strip()
    df["ID"] = df["ID"].fillna("").astype(str).str.strip()
    df["Is a Company"] = df["Is a Company"].fillna("").astype(str).str.strip()
    df["_normalized"] = df["Name"].apply(normalize_name)
    df["_is_company"] = df["Is a Company"].eq("True")
    return df


def match_customer(customer_name: str, contacts_df: pd.DataFrame) -> tuple[str, str]:
    """
    Match a customer name to a contact ID.

    Returns:
        (matched_id, match_info) where match_info describes the match type
    """
    if not customer_name.strip():
        return "", "[No name]"

    name_norm = normalize_name(customer_name)

    # 1. Exact match
    exact = contacts_df[contacts_df["Name"] == customer_name]
    if not exact.empty:
        # Prefer company if multiple
        companies = exact[exact["_is_company"]]
        if not companies.empty:
            return companies.iloc[0]["ID"], "[Exact match, company]"
        return exact.iloc[0]["ID"], "[Exact match]"

    # 2. Normalized match
    norm_matches = contacts_df[contacts_df["_normalized"] == name_norm]
    if not norm_matches.empty:
        companies = norm_matches[norm_matches["_is_company"]]
        if not companies.empty:
            return companies.iloc[0]["ID"], "[Normalized match, company]"
        return norm_matches.iloc[0]["ID"], "[Normalized match]"

    # 3. Fuzzy match - simple contains check first
    contains = contacts_df[contacts_df["_normalized"].str.contains(name_norm, regex=False, na=False)]
    if len(contains) == 1:
        row = contains.iloc[0]
        is_co = "company" if row["_is_company"] else "individual"
        return row["ID"], f"[Contains match, {is_co}]"
    elif len(contains) > 1:
        # Prefer companies
        companies = contains[contains["_is_company"]]
        if len(companies) == 1:
            return companies.iloc[0]["ID"], "[Contains match, company, 1 of many]"
        elif len(companies) > 1:
            # Multiple companies - take shortest name (likely most specific)
            companies = companies.copy()
            companies["_len"] = companies["Name"].str.len()
            best = companies.sort_values("_len").iloc[0]
            return best["ID"], f"[Contains match, company, 1 of {len(companies)}]"
        # No companies, multiple individuals
        return contains.iloc[0]["ID"], f"[Contains match, 1 of {len(contains)}]"

    # 4. Reverse contains - contact name contains customer name
    rev_contains = contacts_df[contacts_df["_normalized"].apply(lambda x: name_norm in x if x else False)]
    if not rev_contains.empty:
        companies = rev_contains[rev_contains["_is_company"]]
        if not companies.empty:
            return companies.iloc[0]["ID"], f"[Partial match, company, 1 of {len(rev_contains)}]"
        return rev_contains.iloc[0]["ID"], f"[Partial match, 1 of {len(rev_contains)}]"

    return "", "[No match]"


def match_all_customers(output_rows: list[dict], contacts_path: Path) -> dict:
    """
    Match all unique customer names in output_rows to contact IDs.

    Returns:
        Dict mapping customer_name -> (matched_id, match_info)
    """
    logging.info("Loading contacts for matching...")
    contacts_df = load_contacts(contacts_path)
    logging.info("Loaded %d contacts (%d companies)", len(contacts_df), contacts_df["_is_company"].sum())

    # Get unique customer names (from rows where Name is non-empty)
    customer_names = set()
    for row in output_rows:
        name = row.get("Name", "").strip()
        if name:
            customer_names.add(name)

    logging.info("Matching %d unique customers...", len(customer_names))

    matches = {}
    stats = {"exact": 0, "normalized": 0, "contains": 0, "partial": 0, "none": 0}

    for name in customer_names:
        matched_id, match_info = match_customer(name, contacts_df)
        matches[name] = (matched_id, match_info)

        # Track stats
        if "Exact" in match_info:
            stats["exact"] += 1
        elif "Normalized" in match_info:
            stats["normalized"] += 1
        elif "Contains" in match_info:
            stats["contains"] += 1
        elif "Partial" in match_info:
            stats["partial"] += 1
        else:
            stats["none"] += 1

    logging.info(
        "Match results: %d exact, %d normalized, %d contains, %d partial, %d unmatched",
        stats["exact"], stats["normalized"], stats["contains"], stats["partial"], stats["none"]
    )

    # Log unmatched names
    if stats["none"] > 0:
        unmatched_names = [name for name, (mid, info) in matches.items() if not mid]
        logging.warning("Unmatched customers: %s", ", ".join(unmatched_names))

    return matches
