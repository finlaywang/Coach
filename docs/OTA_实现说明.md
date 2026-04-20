# OTA 日汇总代码实现说明

## 1. 这套代码解决什么问题

脚本会读取 `Downloads/` 目录下 4 个 OTA 平台导出的订单文件，统一解析后按以下维度汇总：

- 平台 `platform`
- 产品 `product_pid`
- 出发日期 `departure_date`

最后输出用于业务统计的汇总结果，并可按需调用接口上报。

## 2. 代码入口与文件

- 主脚本：`ota_daily_report.py`
- 主要输入目录：`Downloads/`
- 主要输出文件：`ota_daily_sum_items.xlsx`

## 3. 输入文件识别规则

脚本根据文件名自动识别平台：

- KKday：`Orders_Report_*.csv`
- Klook：`bookinglist_-_*.xlsx`
- GYG：`booking-export*.xlsx` / `bookings-export*.xlsx`
- Trip：`*ClientOrder*.xlsx`
- Klook 活动映射：`klook_activities.xlsx`

## 4. 实现逻辑（从输入到输出）

整体流程：

1. 扫描输入目录并按平台归类文件。
2. 分平台解析原始订单，标准化为统一结构 `RowRecord`：
   - `platform`
   - `product_pid`
   - `departure_date`
   - `traveller_count`
   - `has_meal`
   - `lang_code`
3. 按 `(platform, product_pid, departure_date)` 聚合。
4. 生成最终字段：
   - `traveller_count`
   - `has_meal_count`
   - `guide_en_count`
   - `guide_ja_count`
   - `guide_ko_count`
   - `guide_th_count`
   - `guide_vi_count`
5. 写入 Excel；若启用发送，则并发批量 POST。

说明：

- 仅识别并统计 `en/ja/ko/th/vi`。
- 中文相关词（中文/國語/普通话等）不参与语言计数判定，避免“中外文混合时被中文抢占”。
- 含餐人数只统计判定为“含餐”的旅客数量。

## 5. 字段映射与提取关系（表格）

### 5.1 平台输入字段对照表

| 平台 | 文件模式 | 产品ID来源 | 日期来源 | 人数来源 | 含餐判定来源 | 语言判定来源 |
|---|---|---|---|---|---|---|
| KKday | `Orders_Report_*.csv` | `商品编号/商品編號` | `開始日期/开始日期` | `订购总数/訂購總數` | 规格字段优先，其次套餐标题 | 规格字段优先，其次套餐标题 |
| Klook | `bookinglist_-_*.xlsx` | `活動名稱` 通过 `klook_activities.xlsx` 映射 | `使用時間` | `數量` | `方案名稱` | `更多資訊`（偏好语言行） |
| Trip | `*ClientOrder*.xlsx` | `產品 ID/产品 ID/产品ID/產品ID` | `使用日期` | `資源旅客訂單數量/资源旅客订单数量` | `套餐名稱/套餐名称` | `套餐名稱/套餐名称` |
| GYG | `booking-export*.xlsx` / `bookings-export*.xlsx` | `Product` 前缀数字（正则） | `Date` | 多乘客列求和 | `Option` | `Language` |

### 5.2 输出字段来源对照表

| 输出字段 | 计算方法 |
|---|---|
| `platform` | 标准化记录原值 |
| `product_pid` | 标准化记录原值 |
| `departure_date` | 标准化记录原值（`YYYY-MM-DD`） |
| `traveller_count` | 同分组内 `traveller_count` 求和 |
| `has_meal_count` | 同分组内 `has_meal=True` 的 `traveller_count` 求和 |
| `guide_en_count` | 同分组内 `lang_code='en'` 的 `traveller_count` 求和 |
| `guide_ja_count` | 同分组内 `lang_code='ja'` 的 `traveller_count` 求和 |
| `guide_ko_count` | 同分组内 `lang_code='ko'` 的 `traveller_count` 求和 |
| `guide_th_count` | 同分组内 `lang_code='th'` 的 `traveller_count` 求和 |
| `guide_vi_count` | 同分组内 `lang_code='vi'` 的 `traveller_count` 求和 |

### 5.3 语言映射规则（当前实现）

| 目标代码 | 识别关键词（示例） |
|---|---|
| `en` | 英語、英文、English |
| `ja` | 日語、日文、Japanese |
| `ko` | 韓語、韩语、韓文、韩文、Korean |
| `vi` | 越南語、越南语、Vietnamese |
| `th` | 泰語、泰文、Thai |

不参与识别：中文、國語、国语、普通话、普通話、Chinese。

## 6. 各平台解析方法（业务可读版）

### 5.1 KKday
- 从订单列提取产品 ID、日期、人数、套餐名、规格。
- 优先依据“规格”判断含餐；规格不明确时再用套餐标题判断。
- 语言从规格/套餐文本中识别（如英文、日文、韩文等）。

### 5.2 Klook
- 读取活动名称、方案名称、日期、人数、更多资讯。
- 通过 `klook_activities.xlsx` 把活动名称映射到产品 ID。
- 含餐主要依据方案名称关键词判断。
- 语言主要从“更多资讯”中的偏好语言提取。

### 5.3 Trip
- 自动定位真实表头行（不是固定第一行）。
- 提取产品 ID、使用日期、订单人数、套餐名称。
- 从套餐名称中判断含餐与语言。

### 5.4 GYG
- 从 `Product` 文本提取产品 ID（优先前缀数字）。
- 人数为多种乘客类型列的总和。
- `Option` 含 Breakfast/Lunch/Dinner 等关键词判定为含餐。
- 语言从 `Language` 文本映射。

## 7. 运行方法

### 6.1 默认模式（推荐）
仅生成 Excel，不发送接口：

```bash
python3 ota_daily_report.py --downloads-dir Downloads --dry-run
```

### 6.2 查看详细日志

```bash
python3 ota_daily_report.py --downloads-dir Downloads --dry-run --verbose
```

### 6.3 启用接口发送

```bash
python3 ota_daily_report.py \
  --downloads-dir Downloads \
  --enable-post \
  --endpoint http://localhost:8080/api/v1/internal/ota/daily-sum
```

可选参数：

- `--post-workers`：并发数（默认 6）
- `--post-batch-size`：每批发送条数（默认 200）
- `--timeout`：请求超时秒数
- `--lark-webhook`：通知 webhook（可覆盖默认值）

## 8. 输出结果与验收

运行成功后会产生：

- `ota_daily_sum_items.xlsx`（汇总结果）

命令行会输出：

- 识别到的文件数量
- 标准化记录数
- 聚合条目数
- POST 成功/失败统计（仅发送模式）

## 9. 已有回归测试（最小集）

测试文件：`tests/test_ota_daily_report.py`

已覆盖：

- 四个平台基础解析
- Trip 表头自动识别
- 聚合口径正确性（人数、含餐、语言计数）

执行：

```bash
python3 -m pytest -q tests/test_ota_daily_report.py
```

## 10. 给产品的使用建议

- 日常跑数建议使用 `--dry-run`，先确认 Excel 结果。
- 需要对接接口时再加 `--enable-post`。
- 每次新增平台字段或规则后，先补测试再上线。
