#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import secrets
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd


OUTPUT_COLUMNS = [
    "lion_id",
    "lion_title",
    "lion_short_title",
    "kkday_id",
    "kkday_title",
    "kkday_private_id",
    "kkday_private_title",
    "klook_id",
    "klook_title",
    "gyg_id",
    "gyg_title",
    "trip_id",
    "trip_title",
]


def normalize_id(value: object) -> str:
    if pd.isna(value):
        return ""
    s = str(value).replace("\u3000", " ").strip()
    s = re.sub(r"\s+", "", s)
    if not s or s.lower() == "nan":
        return ""
    if re.fullmatch(r"[+-]?\d+\.0+", s):
        s = s.split(".")[0]
    return s


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\u3000", " ").strip()


def add_lookup(lookup: Dict[str, str], product_id: object, name: object, aliases: Optional[Iterable[str]] = None) -> None:
    pid = normalize_id(product_id)
    title = normalize_text(name)
    if not pid or not title:
        return
    lookup.setdefault(pid, title)
    if aliases:
        for alias in aliases:
            if alias:
                lookup.setdefault(alias, title)


def build_product_lookups(products_dir: Path) -> Dict[str, Dict[str, str]]:
    lookups = {
        "kkday": {},
        "klook": {},
        "gyg": {},
        "trip": {},
    }

    # kkday private and group product lists
    for pattern in ("kkday-private-*.xlsx", "kkday_group-*.xlsx"):
        for f in sorted(products_dir.glob(pattern)):
            df = pd.read_excel(f, sheet_name=0)
            for _, row in df.iterrows():
                add_lookup(lookups["kkday"], row.get("id"), row.get("name"))

    # kkday supplier export
    supplier_file = next(iter(sorted(products_dir.glob("kkday_suppliers_*.xlsx"))), None)
    if supplier_file:
        xls = pd.ExcelFile(supplier_file)
        for sheet in xls.sheet_names:
            if "商品列表" not in sheet:
                continue
            df = pd.read_excel(supplier_file, sheet_name=sheet)
            for _, row in df.iterrows():
                add_lookup(lookups["kkday"], row.get("商品平台ID"), row.get("商品名称"))

    klook_file = next(iter(sorted(products_dir.glob("klook-*.xlsx"))), None)
    if klook_file:
        df = pd.read_excel(klook_file, sheet_name=0)
        for _, row in df.iterrows():
            add_lookup(lookups["klook"], row.get("活动 id"), row.get("活动名称"))

    gyg_file = next(iter(sorted(products_dir.glob("getyourguide-*.xlsx"))), None)
    if gyg_file:
        df = pd.read_excel(gyg_file, sheet_name=0)
        for _, row in df.iterrows():
            # Prefer explicit product id when available, fall back to reference code.
            explicit_pid = normalize_id(row.get("产品id"))
            ref_code = normalize_id(row.get("reference_code"))
            pid = explicit_pid or ref_code
            aliases = []
            if ref_code and ref_code != pid:
                aliases.append(ref_code)
            if pid.upper().startswith("T-"):
                aliases.append(pid[2:])
            elif pid.isdigit():
                aliases.append(f"T-{pid}")
            if ref_code.upper().startswith("T-"):
                aliases.append(ref_code[2:])
            elif ref_code.isdigit():
                aliases.append(f"T-{ref_code}")
            add_lookup(lookups["gyg"], pid, row.get("name"), aliases=aliases)

    trip_file = next(iter(sorted(products_dir.glob("ctrip-*.xlsx"))), None)
    if trip_file:
        df = pd.read_excel(trip_file, sheet_name=0)
        for _, row in df.iterrows():
            add_lookup(lookups["trip"], row.get("id"), row.get("名称"))

    return lookups


def build_gyg_product_id_map(products_dir: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    gyg_file = next(iter(sorted(products_dir.glob("getyourguide-*.xlsx"))), None)
    if not gyg_file:
        return mapping

    df = pd.read_excel(gyg_file, sheet_name=0)
    for _, row in df.iterrows():
        explicit_pid = normalize_id(row.get("产品id"))
        ref_code = normalize_id(row.get("reference_code"))

        # Canonical product id for GYG export.
        if explicit_pid:
            canonical = explicit_pid
        elif ref_code.upper().startswith("T-") and ref_code[2:].isdigit():
            canonical = ref_code[2:]
        else:
            canonical = ref_code

        if not canonical:
            continue

        mapping.setdefault(canonical, canonical)
        if ref_code:
            mapping.setdefault(ref_code, canonical)
            if ref_code.upper().startswith("T-") and ref_code[2:].isdigit():
                mapping.setdefault(ref_code[2:], canonical)
        if explicit_pid:
            mapping.setdefault(explicit_pid, canonical)
            mapping.setdefault(f"T-{explicit_pid}", canonical)
    return mapping


def choose_title(raw_title: object, product_id: object, platform_lookup: Dict[str, str]) -> str:
    given = normalize_text(raw_title)
    if given:
        return given
    pid = normalize_id(product_id)
    if not pid:
        return ""
    return platform_lookup.get(pid, "")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export OTA mapping from mapping excel to ota-mapping.csv")
    parser.add_argument("--mapping-file", default="位控表mapping及特殊规则.xlsx")
    parser.add_argument("--sheet", default="團控對齊參考表")
    parser.add_argument("--products-dir", default="products")
    parser.add_argument("--output", default="ota-mapping.csv")
    args = parser.parse_args()

    mapping_file = Path(args.mapping_file)
    products_dir = Path(args.products_dir)
    output_file = Path(args.output)

    mapping_df = pd.read_excel(mapping_file, sheet_name=args.sheet)
    lookups = build_product_lookups(products_dir)
    gyg_pid_map = build_gyg_product_id_map(products_dir)

    rows = []
    for _, row in mapping_df.iterrows():
        kkday_private_id = normalize_id(row.get("kkday專屬團 商品編號"))
        kkday_id = normalize_id(row.get("kkday自控團 商品編號"))
        klook_id = normalize_id(row.get("klook 商品編號"))
        gyg_id_raw = normalize_id(row.get("GYG 商品編號"))
        gyg_id = gyg_pid_map.get(gyg_id_raw, gyg_id_raw)
        if gyg_id.upper().startswith("T-") and gyg_id[2:].isdigit():
            gyg_id = gyg_id[2:]
        lion_short_title = normalize_text(row.get("位控表Sheet名稱"))
        if lion_short_title == "LION 富士三湖" and gyg_id == "526791":
            gyg_id = "1068316"
        trip_id = normalize_id(row.get("TRIP 商品編號"))

        if not any([kkday_private_id, kkday_id, klook_id, gyg_id, trip_id]):
            continue

        out = {
            "lion_id": f"L{secrets.token_hex(4)}",
            "lion_title": normalize_text(row.get("SERP標準團名")),
            "lion_short_title": lion_short_title,
            "kkday_id": kkday_id,
            "kkday_title": choose_title(row.get("kkday自控團 商品名稱"), kkday_id, lookups["kkday"]),
            "kkday_private_id": kkday_private_id,
            "kkday_private_title": choose_title(row.get("kkday專屬團 商品名稱 "), kkday_private_id, lookups["kkday"]),
            "klook_id": klook_id,
            "klook_title": choose_title(row.get("klook 商品名稱"), klook_id, lookups["klook"]),
            "gyg_id": gyg_id,
            "gyg_title": choose_title(row.get("GYG商品名稱"), gyg_id, lookups["gyg"]),
            "trip_id": trip_id,
            "trip_title": choose_title(row.get("TRIP 商品名稱"), trip_id, lookups["trip"]),
        }
        rows.append(out)

    out_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    out_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Exported {len(out_df)} rows to {output_file}")


if __name__ == "__main__":
    main()
