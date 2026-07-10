# Data Audit - 2026-07-09

## Wait-time snapshots

- Rows: **6,417** across **93** rides
- Range: **2026-07-03 -> 2026-07-09** (6 days)
- Distinct snapshot hours: **65** (a perfect hourly run would be ~144 -> ~45% hour coverage)

## Gap days (fewer than 20 snapshot-hours)

- 2026-07-03: 6 hours
- 2026-07-04: 12 hours
- 2026-07-05: 11 hours
- 2026-07-06: 8 hours
- 2026-07-07: 9 hours
- 2026-07-08: 10 hours
- 2026-07-09: 9 hours

7 gap day(s) - check Actions history / migration seam; decide exclude-vs-keep per day.

## Status distribution

- OPERATING: 4,165 rows (64.9%), standby_wait present 69.9%
- CLOSED: 2,050 rows (31.9%), standby_wait present 0.0%
- DOWN: 166 rows (2.6%), standby_wait present 0.0%
- REFURBISHMENT: 36 rows (0.6%), standby_wait present 0.0%

## Ride coverage (top 20 by usable rows: OPERATING + non-null wait)

| ride | park | usable rows | median wait | p95 |
|---|---|---|---|---|
| Prince Charming Regal Carrousel | 75ea578a | 53 | 5 | 10 |
| "it's a small world" | 75ea578a | 53 | 10 | 20 |
| Peter Pan's Flight | 75ea578a | 53 | 40 | 55 |
| Mickey's PhilharMagic | 75ea578a | 53 | 10 | 15 |
| Mad Tea Party | 75ea578a | 53 | 5 | 15 |
| Space Mountain | 75ea578a | 52 | 35 | 50 |
| Buzz Lightyear’s Space Ranger Spin | 75ea578a | 52 | 30 | 65 |
| Tomorrowland Transit Authority PeopleMover | 75ea578a | 52 | 5 | 20 |
| Haunted Mansion | 75ea578a | 52 | 20 | 30 |
| Walt Disney's Enchanted Tiki Room | 75ea578a | 51 | 10 | 10 |
| TRON Lightcycle / Run | 75ea578a | 51 | 55 | 90 |
| Pirates of the Caribbean | 75ea578a | 51 | 15 | 25 |
| Tomorrowland Speedway | 75ea578a | 50 | 15 | 25 |
| Mission: SPACE | 47f90d2c | 50 | 15 | 25 |
| Guardians of the Galaxy: Cosmic Rewind | 47f90d2c | 50 | 65 | 115 |
| Spaceship Earth | 47f90d2c | 49 | 10 | 20 |
| Journey Into Imagination With Figment | 47f90d2c | 49 | 5 | 25 |
| Under the Sea - Journey of The Little Mermaid | 75ea578a | 49 | 10 | 35 |
| Gran Fiesta Tour Starring The Three Caballeros | 47f90d2c | 49 | 5 | 10 |
| Country Bear Musical Jamboree | 75ea578a | 49 | 10 | 10 |

Rides with <500 usable rows (candidates to exclude): **66**

## Forecast table (benchmark: beat the API's own predictions)

- 40,242 forecast rows for 63 rides, 2026-07-03 -> 2026-07-09

## Weather table (Phase 2 features)

- 68 rows, 2026-07-03 -> 2026-07-09

## Target definition (Phase 1+)

- Predict `standby_wait` at T+1 hour, per ride, using data <= T
- Training rows restricted to `status = 'OPERATING'` and non-null standby_wait
- Rides under the usable-rows floor excluded (list above)
- Time-ordered splits only; final N weeks held out for testing
