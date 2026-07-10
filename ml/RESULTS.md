# Model — walk-forward validation (Phase 3)

> **!! ONLY 6.9 DAYS OF HISTORY (threshold: 60). These numbers prove the pipeline runs; they are NOT claimable performance metrics. Re-run when the warehouse matures.**

History: **6.9 days** · gradient boosting (XGBoost, MAE objective) · 3 expanding-window folds over the final 40% of the time range · 268 total test rows

**Model does NOT beat persistence (5.05 vs 4.96 min MAE) — a lesson, not a bullet. Likely insufficient history for the lag features to carry signal.**

## Per-fold comparison

| fold                      |   train_rows |   test_rows |   model_mae |   persistence_mae |
|:--------------------------|-------------:|------------:|------------:|------------------:|
| 07-07 17:36 → 07-08 10:12 |          410 |         114 |        5.55 |              5.75 |
| 07-08 10:12 → 07-09 02:48 |          524 |         122 |        4.39 |              4.55 |
| 07-09 19:24 → 07-10 12:00 |          646 |          32 |        5.77 |              3.75 |

## Feature importance (mean across folds)

|               |   importance |
|:--------------|-------------:|
| ride_id       |       0.1488 |
| roll_mean_3h  |       0.1368 |
| roll_mean_24h |       0.0789 |
| avg_wait      |       0.0777 |
| day_of_week   |       0.0704 |
| roll_max_24h  |       0.0634 |
| hour_of_day   |       0.0623 |
| lag_1h        |       0.0615 |
| lag_3h        |       0.0614 |
| lag_24h       |       0.061  |
| park_name     |       0.0602 |
| is_weekend    |       0.0591 |
| lag_2h        |       0.0585 |
| lag_168h      |       0      |

