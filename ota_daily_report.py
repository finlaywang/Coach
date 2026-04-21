#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import time
import warnings
from collections import defaultdict
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
DEFAULT_TIMEOUT_SEC = 60.0

PAD_ENV_ID = "Default-217d672b-4f71-439b-b886-cf526beaa100"
PAD_SIGNAL_DIR = r"C:\temp"
PAD_TIMEOUT_SEC = 1200
PAD_POLL_INTERVAL = 5
PAD_FLOWS = [
    {"name": "kkday",         "flow_id": "d1048b69-0d56-4cf2-8780-a8b76eb74f0d"},
    {"name": "kkday_private", "flow_id": "a7754e52-7a38-f111-88b4-6045bd1ff239"},
    {"name": "klook",         "flow_id": "93e77ca5-a3d2-47db-89c5-9e546786527d"},
    {"name": "gyg",           "flow_id": "68f530f2-886a-4730-b3be-9ea0b1b947d0"},
    {"name": "trip",          "flow_id": "acf87cd0-6c1e-4bba-bf35-127f4801bfa2"},
]


@dataclass
class RowRecord:
    platform: str
    product_pid: str
    departure_date: str
    traveller_count: int
    has_meal: bool
    lang_code: Optional[str]


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

KKDAY_PRIVATE_PLAN_OVERRIDES: Dict[str, List[str]] = {
    "25703": [
        "中文導覽服務｜河口湖・新倉山淺間公園・忍野八海・大石公園｜銀座出發",
        "富士山五合目&忍野八海&富士全景纜車｜新宿出發",
    ],
    "155289": [
        "天橋立三景一日遊｜天橋立&伊根灣遊船&美山茅屋之里｜大阪出發",
        "天橋立海之京都一日遊｜伊根灣遊船・伊根舟屋・天橋立｜大阪/京都出發",
    ],
}


def kkday_meal_from_specs(spec_text: str) -> Optional[bool]:
    if not spec_text:
        return None
    if any(k in spec_text for k in KKDAY_SPEC_ADDON_TOKENS):
        if "不加購" in spec_text:
            return False
        if "加購" in spec_text:
            return True
    if any(k in spec_text for k in KKDAY_SPEC_INCLUDE_TOKENS) and "含" in spec_text:
        return "不" not in spec_text
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


def parse_kkday(files: List[str], platform: str) -> List[RowRecord]:
    out: List[RowRecord] = []
    lang_map = [
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
        ("中/日文", "ja"),
        ("中/日語", "ja"),
    ]
    for f in files:
        df = pd.read_csv(f)
        pid_col = pick_col(df, ["商品編號", "商品编号"])
        date_col = pick_col(df, ["開始日期", "开始日期"])
        cnt_col = pick_col(df, ["訂購總數", "订购总数"])
        status_col = pick_col(df, ["訂單狀態", "订单状态"])
        pkg_col = pick_col(df, ["套餐名稱", "套餐名称"])
        product_col = pick_col(df, ["商品名稱", "商品名称"])
        spec_cols = [c for c in ["規格一", "規格二", "規格三", "规格一", "规格二", "规格三"] if c in df.columns]
        if not (pid_col and date_col and cnt_col and pkg_col):
            continue

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

            lang_src = spec_text if spec_text else package_title
            lang_code = parse_lang_from_text(lang_src, lang_map)

            out.append(
                RowRecord(
                    platform=platform,
                    product_pid=pid,
                    departure_date=dep,
                    traveller_count=cnt,
                    has_meal=bool(has_meal),
                    lang_code=lang_code,
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


def parse_klook(files: List[str], activity_map: Dict[str, str]) -> List[RowRecord]:
    out: List[RowRecord] = []
    lang_map = [
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

    for f in files:
        df = pd.read_excel(f)
        date_col = "使用時間" if "使用時間" in df.columns else None
        qty_col = "數量" if "數量" in df.columns else None
        plan_col = "方案名稱" if "方案名稱" in df.columns else None
        activity_col = "活動名稱" if "活動名稱" in df.columns else None
        info_col = "更多資訊" if "更多資訊" in df.columns else None
        status_col = pick_col(df, ["訂單狀態", "订单状态"])
        if not (date_col and qty_col and plan_col and activity_col):
            raise ValueError(
                f"Klook order columns mismatch in {f}, required: 使用時間, 數量, 方案名稱, 活動名稱"
            )

        missing_mapping_count = 0
        for _, row in df.iterrows():
            if status_col and is_cancelled_status(row.get(status_col)):
                continue
            dep = extract_date_yyyy_mm_dd(row.get(date_col))
            cnt = to_int(row.get(qty_col))
            plan_name = norm_text(row.get(plan_col))
            activity_name_raw = norm_text(row.get(activity_col))
            if not dep or cnt <= 0 or not activity_name_raw:
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

            info_text = norm_text(row.get(info_col))
            lang_line = ""
            for line in info_text.splitlines():
                if "偏好語言" in line or "偏好语言" in line:
                    lang_line = line
                    break
            lang_code = parse_lang_from_text(lang_line, lang_map)

            out.append(
                RowRecord(
                    platform="klook",
                    product_pid=pid,
                    departure_date=dep,
                    traveller_count=cnt,
                    has_meal=bool(has_meal),
                    lang_code=lang_code,
                )
            )
        if missing_mapping_count > 0:
            print(f"[WARN] Klook activity mapping missing rows: {missing_mapping_count}; file={os.path.basename(f)}")
    return out


def parse_trip(files: List[str]) -> List[RowRecord]:
    out: List[RowRecord] = []
    lang_map = [
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

    for f in files:
        raw = pd.read_excel(f, header=None)
        header_row = find_trip_header_row(raw)
        header = raw.iloc[header_row].fillna("").astype(str).tolist()
        df = raw.iloc[header_row + 1 :].copy()
        df.columns = header

        pid_col = pick_col(df, ["產品 ID", "产品 ID", "產品ID", "产品ID"])
        date_col = pick_col(df, ["使用日期"])
        cnt_col = pick_col(df, ["資源旅客訂單數量", "资源旅客订单数量"])
        plan_col = pick_col(df, ["套餐名稱", "套餐名称"])
        status_col = pick_col(df, ["訂單狀態", "订单状态", "Order Status"])
        if not (pid_col and date_col and cnt_col and plan_col):
            continue

        for _, row in df.iterrows():
            if status_col and is_cancelled_status(row.get(status_col)):
                continue
            pid = norm_text(row.get(pid_col))
            dep = extract_date_yyyy_mm_dd(row.get(date_col))
            cnt = to_int(row.get(cnt_col))
            plan = norm_text(row.get(plan_col))
            if not pid or not dep or cnt <= 0:
                continue

            has_meal = title_meal_signal(
                plan,
                include_tokens=("含", "包括"),
                meal_tokens=TRIP_TITLE_MEAL_TOKENS,
                colon_shortcut=False,
            ) is True
            lang_code = parse_lang_from_text(plan, lang_map)

            out.append(
                RowRecord(
                    platform="trip",
                    product_pid=pid,
                    departure_date=dep,
                    traveller_count=cnt,
                    has_meal=bool(has_meal),
                    lang_code=lang_code,
                )
            )
    return out


def parse_gyg(files: List[str]) -> List[RowRecord]:
    out: List[RowRecord] = []
    lang_map = {
        "english": "en",
        "japanese": "ja",
        "korean": "ko",
        "vietnamese": "vi",
        "thai": "th",
    }
    meal_tokens = ("breakfast", "lunch", "dinner", "brunch", "linner", "dunch")

    for f in files:
        df = pd.read_excel(f)
        date_col = pick_col(df, ["Date"])
        product_col = pick_col(df, ["Product"])
        option_col = pick_col(df, ["Option"])
        lang_col = pick_col(df, ["Language"])
        if not (date_col and product_col and option_col and lang_col):
            continue

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

            has_meal = any(t in option.lower() for t in meal_tokens)
            lang_text = norm_text(row.get(lang_col)).lower()
            lang_code = None
            for k, v in lang_map.items():
                if k in lang_text:
                    lang_code = v
                    break

            out.append(
                RowRecord(
                    platform="gyg",
                    product_pid=pid,
                    departure_date=dep,
                    traveller_count=cnt,
                    has_meal=has_meal,
                    lang_code=lang_code,
                )
            )
    return out


def aggregate(rows: Iterable[RowRecord]) -> List[Dict[str, object]]:
    acc: Dict[Tuple[str, str, str], Dict[str, object]] = {}
    for r in rows:
        key = (r.platform, r.product_pid, r.departure_date)
        if key not in acc:
            acc[key] = {
                "platform": r.platform,
                "product_pid": r.product_pid,
                "departure_date": r.departure_date,
                "traveller_count": 0,
                "has_meal_count": 0,
                "guide_en_count": 0,
                "guide_ja_count": 0,
                "guide_ko_count": 0,
                "guide_th_count": 0,
                "guide_vi_count": 0,
            }
        payload = acc[key]
        payload["traveller_count"] += r.traveller_count
        if r.has_meal:
            payload["has_meal_count"] += r.traveller_count
        if r.lang_code in {"en", "ja", "ko", "th", "vi"}:
            payload[f"guide_{r.lang_code}_count"] += r.traveller_count
    return list(acc.values())


def persist_items_to_excel(output_excel: str, payloads: List[Dict[str, object]]) -> None:
    df = pd.DataFrame(payloads)
    df.to_excel(output_excel, index=False)


def post_payload_batch(endpoint: str, items: List[Dict[str, object]], timeout_sec: float, reset: bool = False) -> Tuple[bool, str]:
    payload: Dict[str, object] = {"items": items}
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
            body = resp.read(400).decode("utf-8", errors="ignore")
            if 200 <= code < 300:
                return True, f"HTTP {code}"
            return False, f"HTTP {code}: {body}"
    except error.HTTPError as e:
        body = e.read(400).decode("utf-8", errors="ignore")
        return False, f"HTTPError {e.code}: {body}"
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


def wait_for_pad_flows() -> Tuple[bool, List[str]]:
    for f in PAD_FLOWS:
        path = os.path.join(PAD_SIGNAL_DIR, f"flow_{f['name']}.json")
        if os.path.exists(path):
            os.remove(path)

    for f in PAD_FLOWS:
        url = (
            f"ms-powerautomate://console/flow/run"
            f"?environmentid={PAD_ENV_ID}&workflowid={f['flow_id']}&source=Other"
        )
        subprocess.Popen(f'start "" "{url}"', shell=True)
        print(f"[触发] {f['name']}")

        path = os.path.join(PAD_SIGNAL_DIR, f"flow_{f['name']}.json")
        start = time.time()
        while time.time() - start < PAD_TIMEOUT_SEC:
            if os.path.exists(path):
                try:
                    with open(path, encoding="utf-16") as fp:
                        status = json.loads(fp.read()).get("status")
                except Exception:
                    status = None
                if status == "success":
                    print(f"[完成] {f['name']}")
                    break
                else:
                    return False, [f["name"]]
            time.sleep(PAD_POLL_INTERVAL)
        else:
            return False, [f["name"]]

    return True, []


def discover(downloads_dir: str) -> Dict[str, List[str]]:
    all_csv = sorted(glob.glob(os.path.join(downloads_dir, "*.csv")))
    kkday_private_files = [
        p for p in all_csv if os.path.basename(p).lower().startswith("kkday_private")
    ]
    kkday_files = [
        p
        for p in all_csv
        if os.path.basename(p).lower().startswith("kkday_group")
    ]
    return {
        "kkday": kkday_files,
        "kkday_private": kkday_private_files,
        "klook": sorted(p for p in glob.glob(os.path.join(downloads_dir, "bookinglist_-_*.xlsx")) if not os.path.basename(p).startswith("~$")),
        "klook_activities": sorted(p for p in glob.glob(os.path.join(downloads_dir, "klook_activities*.xlsx")) if not os.path.basename(p).startswith("~$")),
        "gyg": sorted(p for p in glob.glob(os.path.join(downloads_dir, "bookings-export*.xlsx")) if not os.path.basename(p).startswith("~$")),
        "trip": sorted(p for p in glob.glob(os.path.join(downloads_dir, "*ClientOrder*.xlsx")) if not os.path.basename(p).startswith("~$")),
    }


def make_archive_dir(input_dir: str) -> str:
    base = datetime.now().strftime("%Y%m%d")
    archive_root = os.path.join(input_dir, "archived")
    candidate = os.path.join(archive_root, base)
    if not os.path.exists(candidate):
        os.makedirs(candidate)
        return candidate
    suffix = 1
    while True:
        candidate = os.path.join(archive_root, f"{base}({suffix})")
        if not os.path.exists(candidate):
            os.makedirs(candidate)
            return candidate
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


def archive_source_files(input_dir: str, files: Dict[str, List[str]]) -> str:
    archive_dir = make_archive_dir(input_dir)
    all_paths = [p for k, paths in files.items() if k != "klook_activities" for p in paths]
    for src in all_paths:
        dst = os.path.join(archive_dir, os.path.basename(src))
        os.rename(src, dst)
    return archive_dir


def run(
    input_dir: str,
    endpoint: str,
    verbose: bool,
    output_excel: str,
    enable_post: bool,
    post_batch_size: int,
) -> int:
    files = discover(input_dir)
    kkday_files = files.get("kkday", [])
    kkday_private_files = files.get("kkday_private", [])
    missing_docs: List[str] = []
    if len(kkday_files) < 1:
        missing_docs.append("kkday")
    if len(kkday_private_files) < 1:
        missing_docs.append("kkday_private")
    if len(files.get("klook", [])) < 1:
        missing_docs.append("klook")
    if len(files.get("gyg", [])) < 1:
        missing_docs.append("gyg")
    if len(files.get("trip", [])) < 1:
        missing_docs.append("trip")
    if len(files.get("klook_activities", [])) < 1:
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
                f"文件统计: kkday={len(kkday_files)}, kkday_private={len(kkday_private_files)}, "
                f"klook={len(files.get('klook', []))}, "
                f"gyg={len(files.get('gyg', []))}, trip={len(files.get('trip', []))}, "
                f"klook_activities={len(files.get('klook_activities', []))}"
            ),
            timeout_sec=DEFAULT_TIMEOUT_SEC,
        )
        print(f"[错误] {missing_msg}")
        return 1
    klook_activity_map = load_klook_activity_map(input_dir)

    rows: List[RowRecord] = []
    rows.extend(parse_kkday(files["kkday"], platform="kkday"))
    rows.extend(parse_kkday(files["kkday_private"], platform="kkday_private"))
    rows.extend(parse_klook(files["klook"], klook_activity_map))
    rows.extend(parse_gyg(files["gyg"]))
    rows.extend(parse_trip(files["trip"]))

    payloads = aggregate(rows)
    payloads.sort(key=lambda x: (x["platform"], x["product_pid"], x["departure_date"]))
    persist_items_to_excel(output_excel, payloads)
    output_abs_path = os.path.abspath(output_excel)

    if verbose:
        by_platform = defaultdict(int)
        for p in payloads:
            by_platform[p["platform"]] += 1
        print(f"[信息] 文件统计: { {k: len(v) for k, v in files.items()} }")
        print(f"[信息] 标准化记录数: {len(rows)}")
        print(f"[信息] 聚合条目数: {len(payloads)}")
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
        if failed_count == 0:
            archive_dir = archive_source_files(input_dir, files)
            print(f"[信息] 源文件已归档至: {archive_dir}")
        print(f"[结果] 已写入={len(payloads)} 已发送={ok_count} 失败={failed_count}")
        print(f"导出文件: file://{output_abs_path}")
    else:
        print("[信息] 当前未启用POST，仅导出Excel；如需启用POST，请添加参数 --post。")
        print(f"[结果] 已写入={len(payloads)} 已跳过发送={len(payloads)} 失败=0")
        print(f"导出文件: file://{output_abs_path}")
    send_lark_notification(
        DEFAULT_LARK_WEBHOOK,
        "处理完成",
        (
            f"目录: {os.path.abspath(input_dir)}\n"
            f"文件统计: { {k: len(v) for k, v in files.items()} }\n"
            f"标准化记录: {len(rows)}\n"
            f"聚合条目: {len(payloads)}\n"
            f"输出: {output_abs_path}\n"
            f"POST启用: {enable_post}\n"
            f"POST批大小: {post_batch_size if post_batch_size > 0 else len(payloads)}"
        ),
        timeout_sec=DEFAULT_TIMEOUT_SEC,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="OTA daily summary aggregator and reporter")
    parser.add_argument("-i", "--input-dir", default=r"C:\Users\admin\Downloads", help="input directory for OTA exports")
    parser.add_argument("--no-dev", action="store_true", default=False, help=f"use prod endpoint instead of dev: {DEFAULT_POST_TARGET}")
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
    args = parser.parse_args()

    endpoint = DEFAULT_POST_TARGET if args.no_dev else DEV_POST_TARGET

    output_excel = args.output_excel or os.path.join(
        r"C:\RPA",
        f"ota_daily_summary_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx",
    )

    if args.pad:
        clear_input_dir(args.input_dir)
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
