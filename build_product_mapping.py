#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict

import pandas as pd


KKDAY_PRIVATE_ID_REMAP: Dict[str, str] = {
    "25703": "25703-sp1",
    "155289": "155289-sp1",
}


GYG_CODE_REMAP: Dict[str, str] = {
    "121946": "1121942",
}

KKDAY_PRIVATE_SP2_PATCH: Dict[str, Dict[str, str]] = {
    "系JP東京富士山五合目1日": {
        "kkday_private_id": "25703-sp2",
    },
    "系JP大阪天橋立伊根1日": {
        "kkday_private_id": "155289-sp2",
    },
}

OUTPUT_COLUMNS = [
    "serp_title",
    "lion_title",
    "lion_short_title",
    "kkday_id",
    "kkday_private_id",
    "klook_id",
    "gyg_id",
    "trip_id",
]

COLUMN_RENAME = {
    "serp_title": "標準團名",
    "lion_title": "雄獅商品名稱",
    "lion_short_title": "雄獅商品代號",
    "kkday_id": "KKday商品ID",
    "kkday_private_id": "KKday專屬團商品ID",
    "klook_id": "Klook活動ID",
    "gyg_id": "GYG Product ID",
    "trip_id": "Trip產品ID",
}


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


def build_lion_title_map(path: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not path.exists():
        return mapping
    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path, sheet_name=0, dtype=str)
    else:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    for _, row in df.iterrows():
        std = str(row.get("標準團名") or "").strip()
        title = str(row.get("商品名") or "").strip()
        if std and title and title != "商品名":
            mapping[std] = title
    return mapping


def build_gyg_product_id_map(products_dir: Path) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    gyg_file = next(iter(sorted(products_dir.glob("getyourguide-*.xlsx"))), None)
    if not gyg_file:
        return mapping

    df = pd.read_excel(gyg_file, sheet_name=0)
    for _, row in df.iterrows():
        explicit_pid = normalize_id(row.get("Tour ID"))
        ref_code = normalize_id(row.get("Reference Code"))

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
            # case-insensitive: store both original and lowercase
            mapping.setdefault(ref_code, canonical)
            mapping.setdefault(ref_code.lower(), canonical)
            if ref_code.upper().startswith("T-") and ref_code[2:].isdigit():
                mapping.setdefault(ref_code[2:], canonical)
        if explicit_pid:
            mapping.setdefault(explicit_pid, canonical)
            mapping.setdefault(f"T-{explicit_pid}", canonical)
    return mapping



def resolve_output_path(path: Path) -> Path:
    if not path.exists():
        return path
    i = 1
    while True:
        candidate = path.with_stem(f"{path.stem}({i})")
        if not candidate.exists():
            return candidate
        i += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Export OTA mapping from mapping excel to ota-mapping.csv")
    parser.add_argument("--mapping-file", default="位控表mapping及特殊规则.xlsx")
    parser.add_argument("--sheet", default="團控對齊參考表")
    parser.add_argument("--products-dir", default="products")
    parser.add_argument("--product-list", default="Coach tour產品一覽及控位表 - 商品列表 のコピー.xlsx")
    parser.add_argument("--output", default="ota-mapping.csv")
    args = parser.parse_args()

    mapping_file = Path(args.mapping_file)
    products_dir = Path(args.products_dir)
    output_file = resolve_output_path(Path(args.output))

    mapping_df = pd.read_excel(mapping_file, sheet_name=args.sheet)
    gyg_pid_map = build_gyg_product_id_map(products_dir)
    lion_title_map = build_lion_title_map(Path(args.product_list))

    rows = []
    for _, row in mapping_df.iterrows():
        kkday_private_raw_id = normalize_id(row.get("kkday專屬團 商品編號"))
        kkday_private_id = KKDAY_PRIVATE_ID_REMAP.get(kkday_private_raw_id, kkday_private_raw_id)
        kkday_id = normalize_id(row.get("kkday自控團 商品編號"))
        klook_id = normalize_id(row.get("klook 商品編號"))
        gyg_id_raw = normalize_id(row.get("GYG 商品編號"))
        gyg_id = gyg_pid_map.get(gyg_id_raw) or gyg_pid_map.get(gyg_id_raw.lower(), gyg_id_raw)
        if gyg_id.upper().startswith("T-") and gyg_id[2:].isdigit():
            gyg_id = gyg_id[2:]
        lion_short_title = normalize_text(row.get("位控表Sheet名稱"))
        trip_id = normalize_id(row.get("TRIP 商品編號"))

        if not any([kkday_private_id, kkday_id, klook_id, gyg_id, trip_id]):
            continue

        serp_title = normalize_text(row.get("SERP標準團名"))
        patch = KKDAY_PRIVATE_SP2_PATCH.get(serp_title)
        if patch:
            kkday_private_id = patch["kkday_private_id"]

        out = {
            "serp_title": serp_title,
            "lion_title": lion_title_map.get(serp_title, ""),
            "lion_short_title": lion_short_title,
            "kkday_id": kkday_id,
            "kkday_private_id": kkday_private_id,
            "klook_id": klook_id,
            "gyg_id": gyg_id,
            "trip_id": trip_id,
        }
        rows.append(out)

    out_df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS).rename(columns=COLUMN_RENAME)
    out_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Exported {len(out_df)} rows to {output_file}")


if __name__ == "__main__":
    main()
