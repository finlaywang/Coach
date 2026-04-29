#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import glob
import json
import os
import re
import shutil
import time
import warnings
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
from urllib import error, request

import pandas as pd

# Suppress noisy openpyxl warning from vendor files lacking default style.
warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style, apply openpyxl's default",
    category=UserWarning,
)

DEFAULT_POST_TARGET = "http://localhost:8080/api/v1/internal/ota/daily-sum"
DEV_POST_TARGET = "http://20.187.191.189/pim/api/v1/internal/ota/daily-sum"
DEFAULT_LARK_WEBHOOK = "https://open.larksuite.com/open-apis/bot/v2/hook/6ebb962d-e817-4b7b-a14c-bb55f53d2413"
SUCCESS_LARK_WEBHOOK = "https://open.larksuite.com/open-apis/bot/v2/hook/b5b6a703-9206-40fb-b533-b11451520a22"
DEFAULT_TIMEOUT_SEC = 60.0

PAD_ENV_ID = "Default-217d672b-4f71-439b-b886-cf526beaa100"
PAD_SIGNAL_DIR = r"C:\RPA\signals"
PAD_TIMEOUT_SEC = 1200
PAD_POLL_INTERVAL = 5
PAD_FILE_SETTLE_SEC = 3
PAD_FLOWS = [
    {"name": "kkday",                  "flow_id": "d1048b69-0d56-4cf2-8780-a8b76eb74f0d"},
    {"name": "kkday_customer",         "flow_id": "362cacd8-b73f-f111-bec6-6045bd1ff239"},
    {"name": "kkday_private",          "flow_id": "a7754e52-7a38-f111-88b4-6045bd1ff239"},
    {"name": "kkday_customer_private", "flow_id": "ba152c77-fb41-f111-bec6-6045bd1ff239"},
    {"name": "klook",         "flow_id": "93e77ca5-a3d2-47db-89c5-9e546786527d"},
    {"name": "gyg",           "flow_id": "68f530f2-886a-4730-b3be-9ea0b1b947d0"},
    {"name": "trip",          "flow_id": "acf87cd0-6c1e-4bba-bf35-127f4801bfa2"},
]
KLOOK_ACTIVITIES_FLOW = {"name": "klook_activities", "flow_id": "01d20731-e238-f111-88b4-6045bd1ff239"}


@dataclass
class Traveller:
    passenger_type: str
    id_type: str
    english_name: str
    chinese_name: str
    gender: str


@dataclass
class OrderRecord:
    platform: str
    platform_order_no: str
    product_pid: str
    departure_date: str
    order_amount: float
    currency: str
    traveller_count: int
    has_meal: bool
    lang_code: Optional[str]
    travellers: List[Traveller]


FEATURED_MEAL_KEYWORDS = ("特色早餐", "特色午餐", "特色晚餐", "鍋", "御膳", "特色料理", "含午餐", "宴")


def resolve_klook_activity_file(downloads_dir: str) -> Optional[str]:
    pattern = os.path.join(downloads_dir, "klook_activities*.xlsx")
    candidates = [
        p
        for p in glob.glob(pattern)
        if not os.path.basename(p).startswith("~$")
    ]
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def norm_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def extract_date_yyyy_mm_dd(value) -> Optional[str]:
    text = norm_text(value)
    if not text:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    for fmt in ("%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def to_int(value, default: int = 0) -> int:
    text = norm_text(value)
    if not text:
        return default
    text = text.replace(",", "")
    try:
        return int(float(text))
    except ValueError:
        return default


def to_float(value, default: float = 0.0) -> float:
    text = norm_text(value).replace(",", "")
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def is_cancelled_status(value) -> bool:
    text = norm_text(value).lower()
    if not text:
        return False
    if text == "取消" or "已取消" in text:
        return True
    return re.search(r"\b(cancelled|canceled)\b", text) is not None


def parse_lang_from_text(text: str, mapping: List[Tuple[str, str]]) -> Optional[str]:
    for token, code in mapping:
        if token in text:
            return code
    return None


KKDAY_SPEC_INCLUDE_TOKENS = ("餐食", "早餐", "午餐", "晚餐", "御膳", "鍋", "料理", "宴")
KKDAY_SPEC_ADDON_TOKENS = ("鍋", "料理", "宴", "餐")

DEFAULT_TITLE_MEAL_TOKENS = ("餐", "套餐", "餐食", "早餐", "午餐", "晚餐", "鍋", "宴", "御膳", "料理")
TRIP_TITLE_MEAL_TOKENS = ("餐", "餐食", "早餐", "午餐", "晚餐", "宴", "鍋", "御膳")

LANG_MAP: List[Tuple[str, str]] = [
    ("英語", "en"),
    ("英文", "en"),
    ("日語", "ja"),
    ("日文", "ja"),
    ("韓語", "ko"),
    ("韩语", "ko"),
    ("韓文", "ko"),
    ("韩文", "ko"),
    ("越南語", "vi"),
    ("越南语", "vi"),
    ("泰語", "th"),
    ("泰文", "th"),
]

KKDAY_PRIVATE_PLAN_OVERRIDES: Dict[str, List[str]] = {
    "25703": [
        "富士山五合目&忍野八海&富士全景纜車｜新宿出發",
    ],
}


def kkday_meal_from_specs(spec_text: str) -> Optional[bool]:
    if not spec_text:
        return None
    if any(k in spec_text for k in KKDAY_SPEC_INCLUDE_TOKENS) and "含" in spec_text:
        return "不" not in spec_text
    if any(k in spec_text for k in KKDAY_SPEC_ADDON_TOKENS):
        if "不加購" in spec_text:
            return False
        if "加購" in spec_text:
            return True
    return None


def title_meal_signal(
    title: str,
    include_tokens=("含",),
    extra_negative_tokens: Tuple[str, ...] = (),
    meal_tokens: Tuple[str, ...] = DEFAULT_TITLE_MEAL_TOKENS,
    colon_shortcut: bool = True,
) -> Optional[bool]:
    if not title:
        return None
    if colon_shortcut and re.search(r"(餐食：|早餐：|午餐：|晚餐：)", title):
        return True
    has_meal_kw = any(k in title for k in meal_tokens)
    if not has_meal_kw:
        return None
    has_include = any(k in title for k in include_tokens)
    if has_include:
        if "不" in title:
            return False
        if any(k in title for k in extra_negative_tokens):
            return False
        return True
    if any(k in title for k in extra_negative_tokens):
        return False
    return None



def find_trip_header_row(raw_df: pd.DataFrame) -> int:
    limit = min(len(raw_df), 30)
    for i in range(limit):
        row_text = "|".join(raw_df.iloc[i].fillna("").astype(str).tolist())
        if ("產品 ID" in row_text or "产品 ID" in row_text) and "使用日期" in row_text:
            return i
    raise ValueError("Trip file header row not found")


def load_kkday_customer(customer_csv: str) -> pd.DataFrame:
    df = pd.read_csv(customer_csv, header=1, encoding="utf-8-sig", dtype=str)
    order_col = pick_col(df, ["訂單編號", "订单编号"])
    if not order_col:
        raise ValueError(f"kkday customer CSV 缺少订单号列（訂單編號/订单编号）: {customer_csv}")
    df[order_col] = df[order_col].ffill()
    df = df[df[order_col].notna() & (df[order_col].str.strip() != "")]
    return df.reset_index(drop=True)


def _kkday_id_type(taiwan_id: str, passport: str) -> str:
    if taiwan_id:
        return "台湾身分证"
    if passport:
        return "护照"
    return ""


def _kkday_gender(value: str) -> str:
    v = norm_text(value).upper()
    return v if v in ("M", "F") else "U"


def _build_kkday_travellers(group_rows: pd.DataFrame, adult: int, child: int, infant: int) -> List[Traveller]:
    rows = group_rows.copy()
    if "旅客生日" in rows.columns:
        rows["_birth_key"] = rows["旅客生日"].fillna("")
    else:
        rows["_birth_key"] = ""
    has_birth = rows["_birth_key"].str.strip() != ""
    if has_birth.any():
        sorted_rows = pd.concat([
            rows[has_birth].sort_values("_birth_key"),
            rows[~has_birth],
        ])
    else:
        sorted_rows = rows
    travellers: List[Traveller] = []
    types = ["成人"] * adult + ["儿童"] * child + ["婴儿"] * infant
    for i, (_, r) in enumerate(sorted_rows.iterrows()):
        pt = types[i] if i < len(types) else "成人"
        first_en = norm_text(r.get("旅客護照名（英文）"))
        last_en = norm_text(r.get("旅客護照姓（英文）"))
        english_name = (f"{last_en} {first_en}").strip() if (first_en or last_en) else ""
        first_zh = norm_text(r.get("名"))
        last_zh = norm_text(r.get("姓氏"))
        chinese_name = (f"{last_zh}{first_zh}").strip()
        taiwan_id = norm_text(r.get("台灣身分證字號"))
        passport = norm_text(r.get("護照號碼"))
        travellers.append(
            Traveller(
                passenger_type=pt,
                id_type=_kkday_id_type(taiwan_id, passport),
                english_name=english_name,
                chinese_name=chinese_name,
                gender=_kkday_gender(r.get("性別")),
            )
        )
    return travellers


def parse_kkday(f: str, platform: str, customer_df: pd.DataFrame) -> List[OrderRecord]:
    out: List[OrderRecord] = []
    df = pd.read_csv(f, encoding="utf-8-sig")
    pid_col = pick_col(df, ["商品編號", "商品编号"])
    date_col = pick_col(df, ["開始日期", "开始日期"])
    cnt_col = pick_col(df, ["訂購總數", "订购总数"])
    status_col = pick_col(df, ["訂單狀態", "订单状态"])
    pkg_col = pick_col(df, ["套餐名稱", "套餐名称"])
    product_col = pick_col(df, ["商品名稱", "商品名称"])
    order_col = pick_col(df, ["訂單編號", "订单编号"])
    cur_col = pick_col(df, ["幣別", "币别"])
    amount_col = pick_col(df, ["成本金額", "成本金额"])
    adult_col = pick_col(df, ["成人"])
    child_col = pick_col(df, ["兒童", "儿童"])
    infant_col = pick_col(df, ["幼童"])
    spec_cols = [c for c in ["規格一", "規格二", "規格三", "规格一", "规格二", "规格三"] if c in df.columns]
    if not (pid_col and date_col and cnt_col and pkg_col):
        return out

    if order_col:
        df[order_col] = df[order_col].ffill()

    cust_order_col = pick_col(customer_df, ["訂單編號", "订单编号"])
    if not cust_order_col:
        raise ValueError("customer_df 缺少订单号列（訂單編號/订单编号）")
    customer_groups = customer_df.groupby(cust_order_col)
    cust_lang_col = pick_col(customer_df, ["導覽語言", "导览语言"])

    for _, row in df.iterrows():
        if status_col and is_cancelled_status(row.get(status_col)):
            continue
        pid = norm_text(row.get(pid_col))
        dep = extract_date_yyyy_mm_dd(row.get(date_col))
        cnt = to_int(row.get(cnt_col))
        if not pid or not dep or cnt <= 0:
            continue
        spec_text = " | ".join(norm_text(row.get(c)) for c in spec_cols if norm_text(row.get(c)))
        package_title = norm_text(row.get(pkg_col))
        product_title = norm_text(row.get(product_col)) if product_col else ""
        if platform == "kkday_private" and pid in KKDAY_PRIVATE_PLAN_OVERRIDES:
            sp2_plans = KKDAY_PRIVATE_PLAN_OVERRIDES[pid]
            pid += "-sp2" if any(p in package_title for p in sp2_plans) else "-sp1"

        meal_by_spec = kkday_meal_from_specs(spec_text)
        if meal_by_spec is not None:
            has_meal = meal_by_spec
        else:
            meal_by_package = title_meal_signal(package_title, include_tokens=("含",))
            if meal_by_package is not None:
                has_meal = meal_by_package
            else:
                has_meal = any(k in product_title for k in FEATURED_MEAL_KEYWORDS)

        oid = norm_text(row.get(order_col))
        if not oid:
            print(f"[警告] kkday 跳过孤儿行（前文无订单号可继承）: pid={pid} dep={dep}")
            continue
        adult_n = to_int(row.get(adult_col)) if adult_col else 0
        child_n = to_int(row.get(child_col)) if child_col else 0
        infant_n = to_int(row.get(infant_col)) if infant_col else 0

        lang_code: Optional[str] = None
        travellers: List[Traveller] = []
        if oid in customer_groups.groups:
            sub = customer_groups.get_group(oid)
            if cust_lang_col:
                raw_lang = norm_text(sub.iloc[0].get(cust_lang_col))
                lang_code = parse_lang_from_text(raw_lang, LANG_MAP) if raw_lang else None
            travellers = _build_kkday_travellers(sub, adult_n, child_n, infant_n)
        else:
            print(f"[警告] 导览语言未匹配: 訂單編號={oid} pid={pid}")

        out.append(
            OrderRecord(
                platform=platform,
                platform_order_no=oid,
                product_pid=pid,
                departure_date=dep,
                order_amount=to_float(row.get(amount_col)) if amount_col else 0.0,
                currency=norm_text(row.get(cur_col)) if cur_col else "",
                traveller_count=cnt,
                has_meal=bool(has_meal),
                lang_code=lang_code,
                travellers=travellers,
            )
        )
    return out


def load_klook_activity_map(downloads_dir: str) -> Dict[str, str]:
    target = resolve_klook_activity_file(downloads_dir)
    if not target:
        raise FileNotFoundError(
            f"Klook activity mapping file not found under {downloads_dir} by pattern 'klook_activities*.xlsx'"
        )

    df = pd.read_excel(target, dtype=str)
    id_col = "activity_id"
    name_col = "activity_name"
    if id_col not in df.columns or name_col not in df.columns:
        raise ValueError(
            f"Invalid mapping columns in {target}, require exact headers: activity_id, activity_name"
        )

    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        activity_id = norm_text(row.get(id_col))
        activity_name = norm_text(row.get(name_col))
        if activity_id and activity_name:
            mapping[activity_name.lower()] = activity_id

    if not mapping:
        raise ValueError(f"Klook activity mapping is empty: {target}")
    return mapping


def _has_klook_unknown_activities(f: str, activity_map: Dict[str, str]) -> bool:
    try:
        df = pd.read_excel(f, usecols=["活動名稱"])
    except Exception:
        return False
    for val in df["活動名稱"].dropna():
        name = norm_text(val)
        if name and name.lower() not in activity_map:
            return True
    return False


KLOOK_TITLE_GENDER = {"mr": "M", "mister": "M", "ms": "F", "mrs": "F", "miss": "F"}


def _klook_gender(value) -> str:
    return KLOOK_TITLE_GENDER.get(norm_text(value).lower(), "U")


def _klook_currency(price_col: Optional[str]) -> str:
    if not price_col:
        return "JPY"
    m = re.search(r"\(([A-Za-z]+)\)", price_col)
    return m.group(1).upper() if m else "JPY"


def parse_klook(f: str, activity_map: Dict[str, str]) -> Tuple[List[OrderRecord], int]:
    out: List[OrderRecord] = []

    df = pd.read_excel(f)
    order_col = pick_col(df, ["訂單編號", "订单编号"])
    date_col = pick_col(df, ["使用時間", "使用时间"])
    plan_col = pick_col(df, ["方案名稱", "方案名称"])
    activity_col = pick_col(df, ["活動名稱", "活动名称"])
    lang_col = pick_col(df, ["10010010-偏好語言", "10010010-偏好语言"])
    status_col = pick_col(df, ["訂單狀態", "订单状态"])
    last_col = pick_col(df, ["10010002-姓氏"])
    first_col = pick_col(df, ["10010003-名字"])
    title_col = pick_col(df, ["10010004-稱謂"])
    price_col = next((c for c in df.columns if str(c).startswith("單價")), None)
    if not (order_col and date_col and plan_col and activity_col):
        raise ValueError(
            f"Klook order columns mismatch in {f}, required: 訂單編號, 使用時間, 方案名稱, 活動名稱"
        )

    currency = _klook_currency(price_col)
    missing_mapping_count = 0

    for oid, group in df.groupby(order_col, sort=False):
        oid_text = norm_text(oid)
        if not oid_text:
            continue
        if status_col and is_cancelled_status(group.iloc[0].get(status_col)):
            continue
        head = group.iloc[0]
        dep = extract_date_yyyy_mm_dd(head.get(date_col))
        plan_name = norm_text(head.get(plan_col))
        activity_name_raw = norm_text(head.get(activity_col))
        if not dep or not activity_name_raw:
            continue
        pid = activity_map.get(activity_name_raw.lower())
        if not pid:
            missing_mapping_count += 1
            continue

        meal_by_plan = title_meal_signal(
            plan_name, include_tokens=("含",), extra_negative_tokens=("自理",)
        )
        if meal_by_plan is not None:
            has_meal = meal_by_plan
        else:
            has_meal = any(k in activity_name_raw for k in FEATURED_MEAL_KEYWORDS)

        lang_code = parse_lang_from_text(norm_text(head.get(lang_col)), LANG_MAP) if lang_col else None

        order_amount = group[price_col].apply(to_float).sum() if price_col else 0.0

        travellers: List[Traveller] = []
        for _, r in group.iterrows():
            last = norm_text(r.get(last_col)) if last_col else ""
            first = norm_text(r.get(first_col)) if first_col else ""
            if not (last or first):
                continue
            travellers.append(
                Traveller(
                    passenger_type="成人",
                    id_type="",
                    english_name=(f"{last} {first}").strip(),
                    chinese_name="",
                    gender=_klook_gender(r.get(title_col)) if title_col else "U",
                )
            )

        out.append(
            OrderRecord(
                platform="klook",
                platform_order_no=oid_text,
                product_pid=pid,
                departure_date=dep,
                order_amount=order_amount,
                currency=currency,
                traveller_count=len(group),
                has_meal=bool(has_meal),
                lang_code=lang_code,
                travellers=travellers,
            )
        )
    if missing_mapping_count > 0:
        print(f"[WARN] Klook activity mapping missing rows: {missing_mapping_count}; file={os.path.basename(f)}")
    return out, missing_mapping_count


TRIP_BCP47_LANG_CODES = {"en", "ja", "ko", "th", "vi"}


def _parse_trip_traveller(text: str) -> Optional[Traveller]:
    if not text:
        return None
    name = norm_text(text.split(",", 1)[0])
    if not name:
        return None
    if "/" in name:
        english_name = name.replace("/", " ").strip()
        chinese_name = ""
    else:
        english_name = ""
        chinese_name = name
    return Traveller(
        passenger_type="成人",
        id_type="",
        english_name=english_name,
        chinese_name=chinese_name,
        gender="U",
    )


def parse_trip(f: str) -> List[OrderRecord]:
    out: List[OrderRecord] = []

    raw = pd.read_excel(f, sheet_name="待辦事項", header=None)
    header_row = find_trip_header_row(raw)
    header = raw.iloc[header_row].fillna("").astype(str).tolist()
    df = raw.iloc[header_row + 1 :].copy()
    df.columns = header

    pid_col = pick_col(df, ["產品 ID", "产品 ID"])
    date_col = pick_col(df, ["使用日期"])
    cnt_col = pick_col(df, ["訂單數量", "订单数量"])
    plan_col = pick_col(df, ["套餐名稱", "套餐名称"])
    lang_col = pick_col(df, ["訂單語言", "订单语言"])
    status_col = pick_col(df, ["訂單狀態", "订单状态"])
    order_col = pick_col(df, ["訂單編號", "订单编号"])
    amount_col = pick_col(df, ["售價總額", "售价总额"])
    cur_col = pick_col(df, ["售價貨幣", "售价货币"])
    pax_col = pick_col(df, ["旅客資料", "旅客资料"])
    if not (pid_col and date_col and cnt_col and plan_col):
        return out

    if order_col:
        df[order_col] = df[order_col].ffill()

    for _, row in df.iterrows():
        if status_col and is_cancelled_status(row.get(status_col)):
            continue
        pid = norm_text(row.get(pid_col))
        dep = extract_date_yyyy_mm_dd(row.get(date_col))
        cnt = to_int(row.get(cnt_col))
        plan = norm_text(row.get(plan_col))
        if not pid or not dep or cnt <= 0:
            continue
        oid = norm_text(row.get(order_col)) if order_col else ""
        if not oid:
            print(f"[警告] trip 跳过孤儿行（前文无订单号可继承）: pid={pid} dep={dep}")
            continue

        has_meal = title_meal_signal(
            plan,
            include_tokens=("含", "包括"),
            meal_tokens=TRIP_TITLE_MEAL_TOKENS,
            colon_shortcut=False,
        ) is True
        prefix = norm_text(row.get(lang_col)).split("-", 1)[0].lower() if lang_col else ""
        lang_code = prefix if prefix in TRIP_BCP47_LANG_CODES else None

        traveller = _parse_trip_traveller(norm_text(row.get(pax_col)) if pax_col else "")
        try:
            amount = float(row.get(amount_col)) if amount_col and pd.notna(row.get(amount_col)) else 0.0
        except (TypeError, ValueError):
            amount = 0.0

        out.append(
            OrderRecord(
                platform="trip",
                platform_order_no=oid,
                product_pid=pid,
                departure_date=dep,
                order_amount=amount,
                currency=norm_text(row.get(cur_col)) if cur_col else "",
                traveller_count=cnt,
                has_meal=bool(has_meal),
                lang_code=lang_code,
                travellers=[traveller] if traveller else [],
            )
        )
    return out


def _parse_gyg_price(value) -> Tuple[float, str]:
    text = norm_text(value)
    if not text:
        return 0.0, "JPY"
    # 支持两种格式："15000 JPY" 和 "JPY 15000"
    m_prefix = re.match(r"([A-Za-z]{2,4})\s*([\d,.]+)", text)
    m_suffix = re.match(r"([\d,.]+)\s*([A-Za-z]{2,4})?", text)
    if m_prefix:
        try:
            amount = float(m_prefix.group(2).replace(",", ""))
        except ValueError:
            amount = 0.0
        currency = m_prefix.group(1).upper()
    elif m_suffix:
        try:
            amount = float(m_suffix.group(1).replace(",", ""))
        except ValueError:
            amount = 0.0
        currency = (m_suffix.group(2) or "JPY").upper()
    else:
        return 0.0, "JPY"
    return amount, currency


def parse_gyg(f: str) -> List[OrderRecord]:
    out: List[OrderRecord] = []
    lang_map = {
        "english": "en",
        "japanese": "ja",
        "korean": "ko",
        "vietnamese": "vi",
        "thai": "th",
    }
    meal_tokens = ("breakfast", "lunch", "dinner", "brunch", "linner", "dunch")

    df = pd.read_excel(f)
    date_col = pick_col(df, ["Date"])
    product_col = pick_col(df, ["Product"])
    option_col = pick_col(df, ["Option"])
    lang_col = pick_col(df, ["Language"])
    order_col = pick_col(df, ["Booking Ref No."])
    price_col = pick_col(df, ["Price"])
    first_col = pick_col(df, ["Traveller's First Name"])
    last_col = pick_col(df, ["Traveller's Surname"])
    if not (date_col and product_col and option_col and lang_col):
        return out

    count_cols = [
        c
        for c in [
            "Adult",
            "Senior",
            "Student (with ID)",
            "EU citizens (with ID)",
            "Student EU citizens (with ID)",
            "Military (with ID)",
            "Youth",
            "Child",
            "Infant",
            "Add-ons",
            "Group",
        ]
        if c in df.columns
    ]

    if not count_cols:
        print(f"[警告] GYG 文件未找到旅客人数列（Adult/Child 等），所有行将被跳过: {os.path.basename(f)}")

    if order_col:
        df[order_col] = df[order_col].ffill()

    for _, row in df.iterrows():
        dep = extract_date_yyyy_mm_dd(row.get(date_col))
        product = norm_text(row.get(product_col))
        option = norm_text(row.get(option_col))
        if not dep or not product:
            continue

        m = re.match(r"(\d+)", product)
        pid = m.group(1) if m else product

        cnt = sum(to_int(row.get(c), 0) for c in count_cols)
        if cnt <= 0:
            continue

        oid = norm_text(row.get(order_col)) if order_col else ""
        if not oid:
            print(f"[警告] gyg 跳过孤儿行（前文无订单号可继承）: pid={pid} dep={dep}")
            continue

        has_meal = any(t in option.lower() for t in meal_tokens)
        lang_text = norm_text(row.get(lang_col)).lower()
        lang_code = None
        for k, v in lang_map.items():
            if k in lang_text:
                lang_code = v
                break

        amount, currency = _parse_gyg_price(row.get(price_col)) if price_col else (0.0, "JPY")

        first = norm_text(row.get(first_col)) if first_col else ""
        last = norm_text(row.get(last_col)) if last_col else ""
        travellers: List[Traveller] = []
        if first or last:
            travellers.append(
                Traveller(
                    passenger_type="成人",
                    id_type="",
                    english_name=(f"{last} {first}").strip(),
                    chinese_name="",
                    gender="U",
                )
            )

        out.append(
            OrderRecord(
                platform="gyg",
                platform_order_no=oid,
                product_pid=pid,
                departure_date=dep,
                order_amount=amount,
                currency=currency,
                traveller_count=cnt,
                has_meal=has_meal,
                lang_code=lang_code,
                travellers=travellers,
            )
        )
    return out


EXCEL_COLUMNS = [
    "platform",
    "platform_order_no",
    "product_pid",
    "departure_date",
    "order_amount",
    "currency",
    "traveller_count",
    "has_meal",
    "lang_code",
]


def order_to_payload(order: OrderRecord) -> Dict[str, object]:
    return {
        "platform": order.platform,
        "platform_order_no": order.platform_order_no,
        "product_pid": order.product_pid,
        "departure_date": order.departure_date,
        "order_amount": order.order_amount,
        "currency": order.currency,
        "traveller_count": order.traveller_count,
        "has_meal": order.has_meal,
        "lang_code": order.lang_code,
        "travellers": [
            {
                "passenger_type": t.passenger_type,
                "id_type": t.id_type,
                "english_name": t.english_name,
                "chinese_name": t.chinese_name,
                "gender": t.gender,
            }
            for t in order.travellers
        ],
    }


def persist_orders_to_excel(output_excel: str, orders: List[OrderRecord]) -> None:
    rows = [
        {col: getattr(o, col) for col in EXCEL_COLUMNS}
        for o in orders
    ]
    df = pd.DataFrame(rows, columns=EXCEL_COLUMNS)
    df.to_excel(output_excel, index=False)


def _interpret_pim_response(http_code: int, body: str) -> Tuple[bool, str]:
    """PIM 后端约定错误也返回 HTTP 200，需解析 body.code 才能判定成败。"""
    if not (200 <= http_code < 300):
        return False, f"HTTP {http_code}: {body[:800]}"
    try:
        parsed = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return False, f"HTTP {http_code} non-JSON body: {body[:800]}"
    api_code = parsed.get("code")
    if api_code in (0, None, "0"):
        return True, f"HTTP {http_code}"
    msg = parsed.get("message") or parsed.get("errors") or body[:800]
    return False, f"HTTP {http_code} api_code={api_code}: {msg}"


def post_payload_batch(endpoint: str, orders: List[Dict[str, object]], timeout_sec: float, reset: bool = False) -> Tuple[bool, str]:
    payload: Dict[str, object] = {"orders": orders}
    if reset:
        payload["reset"] = True
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(
        endpoint,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            code = resp.getcode()
            body = resp.read().decode("utf-8", errors="ignore")
            return _interpret_pim_response(code, body)
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        return False, f"HTTPError {e.code}: {body[:800]}"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def post_payloads(
    endpoint: str,
    payloads: List[Dict[str, object]],
    timeout_sec: float,
    batch_size: int,
) -> Tuple[int, int, List[str]]:
    if not payloads:
        return 0, 0, []

    ok_count = 0
    failed_count = 0
    failed_details: List[str] = []

    real_batch_size = len(payloads) if batch_size <= 0 else max(1, batch_size)
    batches = [
        payloads[i : i + real_batch_size]
        for i in range(0, len(payloads), real_batch_size)
    ]

    for batch_idx, batch in enumerate(batches, start=1):
        is_first = batch_idx == 1
        ok, msg = post_payload_batch(endpoint, batch, timeout_sec, reset=is_first)
        if ok:
            ok_count += len(batch)
        else:
            failed_count += len(batch)
            failed_details.append(
                (
                    f"batch={batch_idx} is_first={is_first} size={len(batch)} error={msg} "
                    f"sample=platform:{batch[0].get('platform')} "
                    f"pid:{batch[0].get('product_pid')} "
                    f"date:{batch[0].get('departure_date')}"
                )
            )
    return ok_count, failed_count, failed_details


def send_lark_notification(webhook: str, title: str, content: str, timeout_sec: float = 10.0) -> Tuple[bool, str]:
    if not webhook:
        return False, "webhook empty"
    message = f"[{title}]\n{content}"
    body = {
        "msg_type": "text",
        "content": {"text": message},
    }
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = request.Request(
        webhook,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            code = resp.getcode()
            resp_text = resp.read(300).decode("utf-8", errors="ignore")
            if 200 <= code < 300:
                return True, f"HTTP {code}"
            return False, f"HTTP {code}: {resp_text}"
    except error.HTTPError as e:
        resp_text = e.read(300).decode("utf-8", errors="ignore")
        return False, f"HTTPError {e.code}: {resp_text}"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def _run_single_pad_flow(flow: dict) -> bool:
    path = os.path.join(PAD_SIGNAL_DIR, f"flow_{flow['name']}.json")
    url = (
        f"ms-powerautomate://console/flow/run"
        f"?environmentid={PAD_ENV_ID}&workflowid={flow['flow_id']}&source=Other"
    )
    os.startfile(url)
    print(f"[触发] {flow['name']}")
    start = time.time()
    while time.time() - start < PAD_TIMEOUT_SEC:
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-16") as fp:
                    status = json.loads(fp.read()).get("status")
            except Exception:
                status = None
            if status == "success":
                print(f"[完成] {flow['name']}")
                return True
            elif status is not None:
                return False
        time.sleep(PAD_POLL_INTERVAL)
    return False


def wait_for_pad_flows() -> Tuple[bool, List[str]]:
    for f in PAD_FLOWS:
        if not _run_single_pad_flow(f):
            return False, [f["name"]]
    return True, []


def discover(downloads_dir: str) -> Dict[str, Optional[str]]:
    def latest(pattern: str) -> Optional[str]:
        candidates = [
            p for p in glob.glob(os.path.join(downloads_dir, pattern))
            if not os.path.basename(p).startswith("~$")
        ]
        return max(candidates, key=os.path.getmtime) if candidates else None

    return {
        "kkday":                    latest("kkday_group*.csv"),
        "kkday_private":            latest("kkday_private*.csv"),
        "kkday_customer":           latest("kkday_customer_group*.csv"),
        "kkday_customer_private":   latest("kkday_customer_private*.csv"),
        "klook":            latest("bookinglist_-_*.xlsx"),
        "klook_activities": latest("klook_activities*.xlsx"),
        "gyg":              latest("bookings-export*.xlsx"),
        "trip":             latest("0G*.xlsx"),
    }


def make_archive_dir(input_dir: str) -> str:
    base = datetime.now().strftime("%Y%m%d")
    archive_root = os.path.join(input_dir, "archived")
    candidate = os.path.join(archive_root, base)
    try:
        os.makedirs(candidate)
        return candidate
    except FileExistsError:
        pass
    suffix = 1
    while True:
        candidate = os.path.join(archive_root, f"{base}({suffix})")
        try:
            os.makedirs(candidate)
            return candidate
        except FileExistsError:
            suffix += 1


def clear_input_dir(input_dir: str) -> None:
    files_to_move = [
        name for name in os.listdir(input_dir)
        if not name.lower().startswith("klook_activities")
        and os.path.isfile(os.path.join(input_dir, name))
    ]
    if not files_to_move:
        return
    archive_dir = make_archive_dir(input_dir)
    for name in files_to_move:
        os.rename(os.path.join(input_dir, name), os.path.join(archive_dir, name))
    print(f"[清空] 已归档旧文件至: {archive_dir}")


def archive_source_files(input_dir: str, files: Dict[str, Optional[str]]) -> str:
    archive_dir = make_archive_dir(input_dir)
    for k, p in files.items():
        if k != "klook_activities" and p:
            os.rename(p, os.path.join(archive_dir, os.path.basename(p)))
    return archive_dir


def run(
    input_dir: str,
    endpoint: str,
    verbose: bool,
    output_excel: str,
    enable_post: bool,
    post_batch_size: int,
    start_time: Optional[datetime] = None,
    enable_pad: bool = False,
    silent: bool = False,
) -> int:
    files = discover(input_dir)
    _required = ["kkday", "kkday_private", "kkday_customer", "kkday_customer_private", "klook", "gyg", "trip"]
    missing_docs = [k for k in _required if not files.get(k)]
    if not files.get("klook_activities") and not enable_pad:
        missing_docs.append("klook_activities")

    if missing_docs:
        missing_msg = (
            "required input files are missing: "
            + ", ".join(missing_docs)
            + f" (input_dir={os.path.abspath(input_dir)})"
        )
        send_lark_notification(
            DEFAULT_LARK_WEBHOOK,
            "文档缺失",
            (
                f"目录: {os.path.abspath(input_dir)}\n"
                f"缺失文档: {', '.join(missing_docs)}\n"
                f"文件统计: "
                + ", ".join(f"{k}={'1' if v else '0'}" for k, v in files.items())
            ),
            timeout_sec=DEFAULT_TIMEOUT_SEC,
        )
        print(f"[错误] {missing_msg}")
        return 1

    klook_activity_map = load_klook_activity_map(input_dir)

    orders: List[OrderRecord] = []
    kkday_customer = load_kkday_customer(files["kkday_customer"])
    kkday_customer_private = load_kkday_customer(files["kkday_customer_private"])
    orders.extend(parse_kkday(files["kkday"], platform="kkday", customer_df=kkday_customer))
    orders.extend(parse_kkday(files["kkday_private"], platform="kkday_private", customer_df=kkday_customer_private))
    # 条件2：预检有未映射活动，先刷新映射再解析
    if enable_pad and _has_klook_unknown_activities(files["klook"], klook_activity_map):
        print("[信息] klook 存在未映射活动，触发 PAD 流刷新映射...")
        if _run_single_pad_flow(KLOOK_ACTIVITIES_FLOW):
            time.sleep(PAD_FILE_SETTLE_SEC)
            klook_activity_map = load_klook_activity_map(input_dir)
        else:
            send_lark_notification(
                DEFAULT_LARK_WEBHOOK,
                "Klook 映射刷新失败",
                "klook_activities PAD 流执行失败，继续使用现有映射，部分 klook 订单可能被丢弃",
                timeout_sec=DEFAULT_TIMEOUT_SEC,
            )
            print("[警告] klook_activities PAD 流执行失败，继续使用现有映射")
    klook_orders, missing_mapping_count = parse_klook(files["klook"], klook_activity_map)
    if missing_mapping_count > 0:
        send_lark_notification(
            DEFAULT_LARK_WEBHOOK,
            "Klook 活动映射缺失",
            f"有 {missing_mapping_count} 条 klook 订单因活动未映射被跳过，请检查 klook_activities 文件",
            timeout_sec=DEFAULT_TIMEOUT_SEC,
        )
    orders.extend(klook_orders)
    orders.extend(parse_gyg(files["gyg"]))
    orders.extend(parse_trip(files["trip"]))

    orders.sort(key=lambda o: (o.platform, o.product_pid, o.departure_date, o.platform_order_no))
    persist_orders_to_excel(output_excel, orders)
    payloads = [order_to_payload(o) for o in orders]
    output_abs_path = os.path.abspath(output_excel)

    if verbose:
        by_platform = defaultdict(int)
        for o in orders:
            by_platform[o.platform] += 1
        print(f"[信息] 文件统计: { {k: (1 if v else 0) for k, v in files.items()} }")
        print(f"[信息] 订单条目数: {len(orders)}")
        print(f"[信息] 各平台条目: {dict(by_platform)}")
        print(f"[信息] 已写入 Excel: {output_abs_path}")

    if enable_post:
        ok_count, failed_count, failed_details = post_payloads(
            endpoint=endpoint,
            payloads=payloads,
            timeout_sec=DEFAULT_TIMEOUT_SEC,
            batch_size=post_batch_size,
        )
        print(
            (
                f"[信息] POST完成: endpoint={endpoint} "
                f"批大小={post_batch_size if post_batch_size > 0 else len(payloads)} "
                f"成功={ok_count} 失败={failed_count} 总数={len(payloads)}"
            )
        )
        if failed_details:
            print("[错误] POST失败明细:")
            for item in failed_details[:20]:
                print(f"  - {item}")
            if len(failed_details) > 20:
                print(f"  - ... 其余 {len(failed_details) - 20} 条省略")
            send_lark_notification(
                DEFAULT_LARK_WEBHOOK,
                "POST批次失败",
                (
                    f"endpoint: {endpoint}\n"
                    f"失败批次数: {len(failed_details)}\n"
                    f"失败条目数: {failed_count}\n"
                    f"明细:\n" + "\n".join(f"  - {d}" for d in failed_details[:10])
                    + (f"\n  - ... 其余 {len(failed_details) - 10} 条省略" if len(failed_details) > 10 else "")
                ),
                timeout_sec=DEFAULT_TIMEOUT_SEC,
            )
        if failed_count == 0 and enable_pad:
            archive_dir = archive_source_files(input_dir, files)
            print(f"[信息] 源文件已归档至: {archive_dir}")
        print(f"[结果] 已写入={len(payloads)} 已发送={ok_count} 失败={failed_count}")
        print(f"导出文件: file://{output_abs_path}")
    else:
        print("[信息] 当前未启用POST，仅导出Excel；如需启用POST，请添加参数 --post。")
        print(f"[结果] 已写入={len(payloads)} 已跳过发送={len(payloads)} 失败=0")
        print(f"导出文件: file://{output_abs_path}")
    end_time = datetime.now()
    elapsed = end_time - (start_time or end_time)
    elapsed_min = int(elapsed.total_seconds()) // 60
    elapsed_sec = int(elapsed.total_seconds()) % 60
    time_str = (
        f"{start_time.strftime('%H:%M')} 到 {end_time.strftime('%H:%M')}"
        f"（耗时 {elapsed_min} 分 {elapsed_sec} 秒）"
        if start_time else f"{end_time.strftime('%H:%M')}"
    )
    platform_counts = Counter(o.platform for o in orders)
    rows_by_platform = (
        f"kkday=报表{platform_counts['kkday']}(订单{len(kkday_customer)}) "
        f"kkday专属团=报表{platform_counts['kkday_private']}(订单{len(kkday_customer_private)}) "
        f"klook={platform_counts['klook']} "
        f"gyg={platform_counts['gyg']} "
        f"trip={platform_counts['trip']}"
    )
    today = end_time.date()
    m3 = today.month + 3
    year_3m = today.year + (m3 - 1) // 12
    month_3m = (m3 - 1) % 12 + 1
    day_3m = min(today.day, calendar.monthrange(year_3m, month_3m)[1])
    end_3m = today.replace(year=year_3m, month=month_3m, day=day_3m)
    date_range_str = f"{today.year}/{today.month}/{today.day} ~ {end_3m.year}/{end_3m.month}/{end_3m.day}"
    send_lark_notification(
        DEFAULT_LARK_WEBHOOK if silent else SUCCESS_LARK_WEBHOOK,
        "处理完成",
        (
            f"订单记录: {rows_by_platform}\n"
            f"订单条目: {len(orders)}\n"
            f"统计时长: {date_range_str}\n"
            f"运行时间: {time_str}\n"
            f"查看结果: https://dev-pim.liontravel.global/zh-TW/ota/daily-sum"
        ),
        timeout_sec=DEFAULT_TIMEOUT_SEC,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="OTA daily summary aggregator and reporter")
    parser.add_argument("-i", "--input-dir", default=os.path.join(os.path.expanduser("~"), "Downloads"), help="input directory for OTA exports")
    parser.add_argument("--local", action="store_true", default=False, help=f"use local endpoint instead of dev: {DEFAULT_POST_TARGET}")
    parser.add_argument("-o", "--output-excel", default=None, help="output excel file path (default: ota_daily_summary_YYYYMMDD_HHMMSS.xlsx in cwd)")
    parser.add_argument(
        "--no-post",
        dest="no_post",
        action="store_true",
        default=False,
        help="disable POST (default: enabled)",
    )
    parser.add_argument("--post-batch-size", type=int, default=200, help="items per POST request; <=0 means all in one request")
    parser.add_argument("-v", "--verbose", action="store_true", help="print details")
    parser.add_argument("--pad", action="store_true", default=False, help="trigger PAD flows and wait before aggregating")
    parser.add_argument("--silent", action="store_true", default=False, help="send success notification to default webhook instead of success webhook")
    args = parser.parse_args()

    endpoint = DEFAULT_POST_TARGET if args.local else DEV_POST_TARGET

    default_basename = f"ota_daily_summary_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    output_excel = args.output_excel or os.path.join(r"C:\RPA", default_basename)
    if os.path.isdir(output_excel):
        output_excel = os.path.join(output_excel, default_basename)
    elif not output_excel.lower().endswith(".xlsx"):
        output_excel += ".xlsx"
    os.makedirs(os.path.dirname(os.path.abspath(output_excel)), exist_ok=True)

    start_time = datetime.now()

    if args.pad:
        clear_input_dir(args.input_dir)
        shutil.rmtree(PAD_SIGNAL_DIR, ignore_errors=True)
        os.makedirs(PAD_SIGNAL_DIR)
        # klook_activities 不随 clear_input_dir 清理，文件缺失时先单独触发
        if not discover(args.input_dir).get("klook_activities"):
            print("[信息] klook_activities 映射文件缺失，触发 PAD 流获取...")
            if not _run_single_pad_flow(KLOOK_ACTIVITIES_FLOW):
                send_lark_notification(
                    DEFAULT_LARK_WEBHOOK,
                    "PAD流执行失败",
                    f"失败/超时平台: {KLOOK_ACTIVITIES_FLOW['name']}",
                    timeout_sec=DEFAULT_TIMEOUT_SEC,
                )
                print(f"[错误] PAD流失败: ['{KLOOK_ACTIVITIES_FLOW['name']}']")
                return 1
            time.sleep(PAD_FILE_SETTLE_SEC)
            if not discover(args.input_dir).get("klook_activities"):
                send_lark_notification(
                    DEFAULT_LARK_WEBHOOK,
                    "PAD流执行失败",
                    f"PAD 报成功但 klook_activities 文件仍未找到: {os.path.abspath(args.input_dir)}",
                    timeout_sec=DEFAULT_TIMEOUT_SEC,
                )
                print("[错误] PAD 流报成功但 klook_activities 文件仍未找到")
                return 1
        ok, failed = wait_for_pad_flows()
        if not ok:
            send_lark_notification(
                DEFAULT_LARK_WEBHOOK,
                "PAD流执行失败",
                f"失败/超时平台: {', '.join(failed)}",
                timeout_sec=DEFAULT_TIMEOUT_SEC,
            )
            print(f"[错误] PAD流失败: {failed}")
            return 1

    try:
        return run(
            input_dir=args.input_dir,
            endpoint=endpoint,
            verbose=args.verbose,
            output_excel=output_excel,
            enable_post=not args.no_post,
            post_batch_size=args.post_batch_size,
            start_time=start_time,
            enable_pad=args.pad,
            silent=args.silent,
        )
    except Exception as e:  # noqa: BLE001
        err_text = f"{type(e).__name__}: {e}"
        send_lark_notification(
            DEFAULT_LARK_WEBHOOK,
            "处理失败",
            (
                f"目录: {os.path.abspath(args.input_dir)}\n"
                f"错误: {err_text}"
            ),
            timeout_sec=DEFAULT_TIMEOUT_SEC,
        )
        print(f"[错误] {err_text}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
