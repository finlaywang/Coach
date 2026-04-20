# OTA Daily Sum Logic

## What It Does
`ota_daily_sum.py` reads OTA export files from `Downloads/`, normalizes order rows, aggregates by:

- `platform`
- `product_pid`
- `departure_date`

Then it:

- always writes `ota_daily_sum_items.xlsx`
- optionally sends batched POST requests when `--enable-post` is set

## Input Discovery
The script scans one directory and matches files by pattern:

- KKday: `Orders_Report_*.csv`
- Klook: `bookinglist_-_*.xlsx`
- GYG: `booking-export*.xlsx` / `bookings-export*.xlsx`
- Trip: `*ClientOrder*.xlsx`
- Klook map: `klook_activities.xlsx`

## Row Normalization
Each platform parser converts rows to a common `RowRecord`:

- `platform`
- `product_pid`
- `departure_date` (`YYYY-MM-DD`)
- `traveller_count`
- `has_meal` (`bool`)
- `lang_code` (`en/ja/ko/th/vi/None`)

Platform-specific rules are applied for:

- meal detection
- language detection
- traveler count extraction

## Aggregation
`aggregate()` merges all normalized rows by `(platform, product_pid, departure_date)` and computes:

- `traveller_count`
- `has_meal_count`
- `guide_en_count`
- `guide_ja_count`
- `guide_ko_count`
- `guide_th_count`
- `guide_vi_count`

Chinese labels are ignored in language mapping and never counted as guide language.

## Runtime Modes
- Dry/default mode: writes Excel only.
- Send mode: add `--enable-post --endpoint <url>` to send `{"items":[...]}` batches.

## Key Functions
- File discovery: `discover()`
- Platform parsers: `parse_kkday()`, `parse_klook()`, `parse_trip()`, `parse_gyg()`
- Aggregation: `aggregate()`
- Output: `persist_items_to_excel()`
- POST: `post_payloads()`
