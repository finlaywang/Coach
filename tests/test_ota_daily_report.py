from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
import ota_daily_report as mod  # noqa: E402


def test_parse_kkday_and_private_platform(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "商品编号": "KK1001",
                "開始日期": "2026/04/30",
                "订购总数": "2",
                "套餐名称": "含早餐行程",
                "规格一": "導覽語言：英文",
            }
        ]
    )
    f1 = tmp_path / "kkday_group_1.csv"
    f2 = tmp_path / "kkday_private_1.csv"
    df.to_csv(f1, index=False)
    df.to_csv(f2, index=False)

    rows = mod.parse_kkday([str(f1)], platform="kkday") + mod.parse_kkday([str(f2)], platform="kkday_private")
    assert len(rows) == 2
    assert rows[0].platform == "kkday"
    assert rows[1].platform == "kkday_private"
    assert rows[0].lang_code == "en"
    assert rows[0].has_meal is True


def test_parse_klook_with_activity_mapping(tmp_path: Path) -> None:
    orders = pd.DataFrame(
        [
            {
                "使用時間": "2026-05-01 09:00:00",
                "數量": 3,
                "方案名稱": "含午餐方案",
                "活動名稱": "Sunset Tour",
                "更多資訊": "偏好語言：日文",
            }
        ]
    )
    order_file = tmp_path / "bookinglist_-_a.xlsx"
    orders.to_excel(order_file, index=False)

    rows = mod.parse_klook([str(order_file)], {"sunset tour": "KL123"})
    assert len(rows) == 1
    assert rows[0].product_pid == "KL123"
    assert rows[0].lang_code == "ja"
    assert rows[0].has_meal is True


def test_parse_trip_header_detection(tmp_path: Path) -> None:
    raw_rows = [
        ["说明", "", "", ""],
        ["", "", "", ""],
        ["產品 ID", "使用日期", "資源旅客訂單數量", "套餐名稱"],
        ["TRIP88", "2026-05-02", 4, "含晚餐 英語導遊"],
    ]
    f = tmp_path / "2761592ClientOrderDetail20260415151511.xlsx"
    pd.DataFrame(raw_rows).to_excel(f, index=False, header=False)

    rows = mod.parse_trip([str(f)])
    assert len(rows) == 1
    assert rows[0].product_pid == "TRIP88"
    assert rows[0].traveller_count == 4
    assert rows[0].lang_code == "en"
    assert rows[0].has_meal is True


def test_parse_gyg_pid_count_meal_and_language(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "Date": "2026-05-03",
                "Product": "1241286 [T-1241286] Sample",
                "Option": "Lunch cruise",
                "Language": "English",
                "Adult": 2,
                "Child": 1,
            }
        ]
    )
    f = tmp_path / "bookings-export (1).xlsx"
    df.to_excel(f, index=False)

    rows = mod.parse_gyg([str(f)])
    assert len(rows) == 1
    assert rows[0].product_pid == "1241286"
    assert rows[0].traveller_count == 3
    assert rows[0].has_meal is True
    assert rows[0].lang_code == "en"


def test_aggregate_merges_by_platform_pid_date() -> None:
    rows = [
        mod.RowRecord("klook", "KL1", "2026-05-01", 2, True, "en"),
        mod.RowRecord("klook", "KL1", "2026-05-01", 1, False, "ja"),
        mod.RowRecord("klook", "KL1", "2026-05-01", 3, True, "zh"),
    ]

    payloads = mod.aggregate(rows)
    assert len(payloads) == 1
    p = payloads[0]
    assert p["traveller_count"] == 6
    assert p["has_meal_count"] == 5
    assert p["guide_en_count"] == 2
    assert p["guide_ja_count"] == 1
    assert p["guide_ko_count"] == 0
    assert p["guide_th_count"] == 0
    assert p["guide_vi_count"] == 0


def test_language_mixed_chinese_and_foreign_prefers_foreign() -> None:
    code = mod.parse_lang_from_text(
        "導覽語言：中文、英文",
        [
            ("英語", "en"),
            ("英文", "en"),
            ("日語", "ja"),
            ("日文", "ja"),
            ("韓語", "ko"),
            ("韩语", "ko"),
            ("越南語", "vi"),
            ("越南语", "vi"),
            ("泰語", "th"),
            ("泰文", "th"),
        ],
    )
    assert code == "en"


def test_cancelled_orders_are_filtered_for_kkday_klook_trip(tmp_path: Path) -> None:
    # KKday
    kkday = pd.DataFrame(
        [
            {"商品编号": "K1", "開始日期": "2026/05/01", "订购总数": 2, "套餐名称": "含早餐", "訂單狀態": "已取消"},
            {"商品编号": "K2", "開始日期": "2026/05/01", "订购总数": 3, "套餐名称": "含早餐", "訂單狀態": "已處理"},
        ]
    )
    kkday_f = tmp_path / "kkday_group_x.csv"
    kkday.to_csv(kkday_f, index=False)
    kkday_rows = mod.parse_kkday([str(kkday_f)], platform="kkday")
    assert len(kkday_rows) == 1
    assert kkday_rows[0].product_pid == "K2"

    # Klook
    klook = pd.DataFrame(
        [
            {"使用時間": "2026-05-01 10:00:00", "數量": 1, "方案名稱": "含午餐", "活動名稱": "Tour A", "訂單狀態": "已取消"},
            {"使用時間": "2026-05-01 10:00:00", "數量": 2, "方案名稱": "含午餐", "活動名稱": "Tour A", "訂單狀態": "已確認"},
        ]
    )
    klook_f = tmp_path / "bookinglist_-_x.xlsx"
    klook.to_excel(klook_f, index=False)
    klook_rows = mod.parse_klook([str(klook_f)], {"tour a": "KL1"})
    assert len(klook_rows) == 1
    assert klook_rows[0].traveller_count == 2

    # Trip
    trip_raw = [
        ["說明", "", "", "", ""],
        ["產品 ID", "使用日期", "資源旅客訂單數量", "套餐名稱", "訂單狀態"],
        ["T1", "2026-05-01", 2, "含晚餐", "Cancelled"],
        ["T2", "2026-05-01", 3, "含晚餐", "pending approval"],
    ]
    trip_f = tmp_path / "xClientOrder.xlsx"
    pd.DataFrame(trip_raw).to_excel(trip_f, index=False, header=False)
    trip_rows = mod.parse_trip([str(trip_f)])
    assert len(trip_rows) == 1
    assert trip_rows[0].product_pid == "T2"


def test_cancelled_status_detection_is_not_overbroad() -> None:
    assert mod.is_cancelled_status("已取消") is True
    assert mod.is_cancelled_status("Cancelled") is True
    assert mod.is_cancelled_status("canceled") is True
    assert mod.is_cancelled_status("可取消") is False
    assert mod.is_cancelled_status("未取消") is False


def test_discover_uses_strict_kkday_prefixes(tmp_path: Path) -> None:
    (tmp_path / "kkday_group_a.csv").write_text("x\n", encoding="utf-8")
    (tmp_path / "kkday_private_a.csv").write_text("x\n", encoding="utf-8")
    (tmp_path / "kkday_a.csv").write_text("x\n", encoding="utf-8")

    found = mod.discover(str(tmp_path))
    assert [Path(p).name for p in found["kkday"]] == ["kkday_group_a.csv"]
    assert [Path(p).name for p in found["kkday_private"]] == ["kkday_private_a.csv"]
