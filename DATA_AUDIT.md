# Data Audit — Wait-Time Prediction (Phase 0)

*Generated 2026-07-10 06:07 UTC by `ml/data_audit.py` against `raw.raw_wait_times`.*

## Verdict

**PAUSE.** Only 6 days of history (threshold: 60). Let the pipeline keep collecting; re-run this audit later. Baselines (Phase 1) could still be prototyped, but no model bullets until real history exists.

## Overview

| Metric | Value |
|---|---|
| Total snapshot rows | 6,603 |
| Usable rows (OPERATING, non-null wait) | 2,910 |
| Distinct attractions | 93 |
| Distinct parks | 4 |
| First snapshot | 2026-07-03 14:43 UTC |
| Last snapshot | 2026-07-10 06:05 UTC |
| History span | 6 days |
| Calendar hours in span | 161 |
| Hours with data | 67 |
| Hours with zero rows (true gaps) | 94 |
| Hours with operating rides (signal hours) | 51 |

## Gap analysis

Zero-row hours are collection failures or the migration seam — the parks being closed does *not* create these, because the workflow runs 24/7 and closed rides still produce rows.

| Gap start (UTC) | Gap end (UTC) | Hours |
|---|---|---|
| 2026-07-10 00:00 | 2026-07-10 04:00 | 5 |
| 2026-07-06 01:00 | 2026-07-06 04:00 | 4 |
| 2026-07-06 06:00 | 2026-07-06 09:00 | 4 |
| 2026-07-04 01:00 | 2026-07-04 03:00 | 3 |
| 2026-07-05 01:00 | 2026-07-05 03:00 | 3 |
| 2026-07-06 11:00 | 2026-07-06 13:00 | 3 |
| 2026-07-07 03:00 | 2026-07-07 05:00 | 3 |
| 2026-07-07 07:00 | 2026-07-07 09:00 | 3 |
| 2026-07-09 01:00 | 2026-07-09 03:00 | 3 |
| 2026-07-09 05:00 | 2026-07-09 07:00 | 3 |
| 2026-07-04 05:00 | 2026-07-04 06:00 | 2 |
| 2026-07-04 08:00 | 2026-07-04 09:00 | 2 |
| 2026-07-05 05:00 | 2026-07-05 06:00 | 2 |
| 2026-07-05 08:00 | 2026-07-05 09:00 | 2 |
| 2026-07-06 15:00 | 2026-07-06 16:00 | 2 |

*…and 38 smaller gaps.*

## Status distribution

| Status | Rows | Null standby_wait |
|---|---|---|
| OPERATING | 4,169 | 1,259 |
| CLOSED | 2,230 | 2,230 |
| DOWN | 166 | 166 |
| REFURBISHMENT | 38 | 38 |

## Target definition

Predict the **standby wait at T+1h, per attraction**, where T is a park-open hour. Ground truth = mean of that attraction's non-null OPERATING `standby_wait` snapshots within hour T+1 (snapshots land ~2x/hour at :23 and :53; averaging them is the hourly grain).

## Inclusion rules

An attraction enters the training set if it has non-null OPERATING waits in at least 50% of its park's open hours.

**Included: 64 attractions. Excluded: 2.**

### Included attractions

| Ride | Park | Usable rows | Coverage |
|---|---|---|---|
| Peter Pan's Flight | Magic Kingdom | 53 | 100.0% |
| Mickey's PhilharMagic | Magic Kingdom | 53 | 100.0% |
| "it's a small world" | Magic Kingdom | 53 | 100.0% |
| Mad Tea Party | Magic Kingdom | 53 | 100.0% |
| Prince Charming Regal Carrousel | Magic Kingdom | 53 | 100.0% |
| Guardians of the Galaxy: Cosmic Rewind | EPCOT | 50 | 100.0% |
| Mission: SPACE | EPCOT | 50 | 100.0% |
| Alien Swirling Saucers | Hollywood Studios | 49 | 100.0% |
| Millennium Falcon: Smugglers Run | Hollywood Studios | 49 | 100.0% |
| Star Tours – The Adventures Continue | Hollywood Studios | 49 | 100.0% |
| The Twilight Zone™ Tower of Terror | Hollywood Studios | 49 | 100.0% |
| Na'vi River Journey | Animal Kingdom | 40 | 100.0% |
| Avatar Flight of Passage | Animal Kingdom | 40 | 100.0% |
| Zootopia: Better Zoogether! | Animal Kingdom | 40 | 100.0% |
| Space Mountain | Magic Kingdom | 52 | 98.0% |
| Tomorrowland Transit Authority PeopleMover | Magic Kingdom | 52 | 98.0% |
| Buzz Lightyear’s Space Ranger Spin | Magic Kingdom | 52 | 98.0% |
| Haunted Mansion | Magic Kingdom | 52 | 98.0% |
| Spaceship Earth | EPCOT | 49 | 97.8% |
| Gran Fiesta Tour Starring The Three Caballeros | EPCOT | 49 | 97.8% |
| Soarin' Across America | EPCOT | 49 | 97.8% |
| Journey Into Imagination With Figment | EPCOT | 49 | 97.8% |
| Kilimanjaro Safaris | Animal Kingdom | 39 | 97.2% |
| Walt Disney's Enchanted Tiki Room | Magic Kingdom | 51 | 95.9% |
| TRON Lightcycle / Run | Magic Kingdom | 51 | 95.9% |
| Pirates of the Caribbean | Magic Kingdom | 51 | 95.9% |
| The Seas with Nemo & Friends | EPCOT | 48 | 95.7% |
| Mickey & Minnie's Runaway Railway | Hollywood Studios | 47 | 95.6% |
| Tomorrowland Speedway | Magic Kingdom | 50 | 93.9% |
| Living with the Land | EPCOT | 47 | 93.5% |
| Turtle Talk With Crush | EPCOT | 47 | 93.5% |
| Frozen Ever After | EPCOT | 47 | 93.5% |
| Toy Story Mania! | Hollywood Studios | 46 | 93.3% |
| Star Wars: Rise of the Resistance | Hollywood Studios | 46 | 93.3% |
| Country Bear Musical Jamboree | Magic Kingdom | 49 | 91.8% |
| Under the Sea - Journey of The Little Mermaid | Magic Kingdom | 49 | 91.8% |
| Dumbo the Flying Elephant | Magic Kingdom | 49 | 91.8% |
| Astro Orbiter | Magic Kingdom | 49 | 91.8% |
| The Magic Carpets of Aladdin | Magic Kingdom | 49 | 91.8% |
| Vacation Fun - An Original Animated Short with Mickey & Minnie | Hollywood Studios | 45 | 91.1% |
| Swiss Family Treehouse | Magic Kingdom | 48 | 89.8% |
| Monsters, Inc. Laugh Floor | Magic Kingdom | 48 | 89.8% |
| The Many Adventures of Winnie the Pooh | Magic Kingdom | 48 | 89.8% |
| Tiana's Bayou Adventure | Magic Kingdom | 48 | 89.8% |
| Rock ’n’ Roller Coaster Starring The Muppets | Hollywood Studios | 44 | 88.9% |
| Jungle Cruise | Magic Kingdom | 47 | 87.8% |
| The Barnstormer | Magic Kingdom | 47 | 87.8% |
| Remy's Ratatouille Adventure | EPCOT | 40 | 87.0% |
| Wildlife Express Train | Animal Kingdom | 35 | 86.1% |
| Expedition Everest - Legend of the Forbidden Mountain | Animal Kingdom | 35 | 86.1% |
| Big Thunder Mountain Railroad | Magic Kingdom | 46 | 85.7% |
| Seven Dwarfs Mine Train | Magic Kingdom | 42 | 79.6% |
| The Hall of Presidents | Magic Kingdom | 42 | 77.6% |
| Maharajah Jungle Trek | Animal Kingdom | 31 | 75.0% |
| Gorilla Falls Exploration Trail | Animal Kingdom | 31 | 75.0% |
| Kali River Rapids | Animal Kingdom | 31 | 75.0% |
| Canada Far and Wide in Circle-Vision 360 | EPCOT | 38 | 73.9% |
| Test Track | EPCOT | 37 | 71.7% |
| Enchanted Tales with Belle | Magic Kingdom | 38 | 69.4% |
| Slinky Dog Dash | Hollywood Studios | 34 | 66.7% |
| Beauty and the Beast Sing-Along | EPCOT | 30 | 65.2% |
| Walt Disney World Railroad - Fantasyland | Magic Kingdom | 34 | 61.2% |
| Walt Disney World Railroad - Main Street, U.S.A. | Magic Kingdom | 34 | 61.2% |
| Reflections of China | EPCOT | 28 | 52.2% |

### Excluded (sparse) attractions

| Ride | Park | Usable rows | Coverage |
|---|---|---|---|
| Walt Disney's Carousel of Progress | Magic Kingdom | 23 | 46.9% |
| Impressions de France | EPCOT | 6 | 13.0% |

