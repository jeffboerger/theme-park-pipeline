# Baselines — next-hour wait prediction (Phase 1)

> **!! ONLY 6.9 DAYS OF HISTORY (threshold: 60). These numbers prove the pipeline runs; they are NOT claimable performance metrics. Re-run when the warehouse matures.**

History: **6.9 days** · train rows: 585 · holdout rows: 93 (time-ordered, cutoff 2026-07-08 18:30 UTC)

| Baseline | Holdout MAE (min) |
|---|---|
| persistence | 4.78 |
| hod_mean | 7.28 |
| seasonal_naive | n/a — only 0% of holdout has a same-hour-last-week match (needs >7 days of history) |

## Per-attraction MAE (minutes)

| ride_name                                                      |   persistence |   hod_mean |
|:---------------------------------------------------------------|--------------:|-----------:|
| Alien Swirling Saucers                                         |           0   |      11.67 |
| Astro Orbiter                                                  |           0   |       9.06 |
| Canada Far and Wide in Circle-Vision 360                       |           0   |       0    |
| Country Bear Musical Jamboree                                  |           0   |       0    |
| Frozen Ever After                                              |           0   |       5    |
| Gorilla Falls Exploration Trail                                |           0   |       0    |
| Dumbo the Flying Elephant                                      |           0   |       2.14 |
| Journey Into Imagination With Figment                          |           0   |       1.67 |
| Mad Tea Party                                                  |           0   |       5.61 |
| Maharajah Jungle Trek                                          |           0   |       0    |
| Monsters, Inc. Laugh Floor                                     |           0   |       1.67 |
| Mission: SPACE                                                 |           0   |       1.14 |
| Kali River Rapids                                              |           0   |      25    |
| Seven Dwarfs Mine Train                                        |           0   |       9    |
| Remy's Ratatouille Adventure                                   |           0   |       0.71 |
| Reflections of China                                           |           0   |       0    |
| Wildlife Express Train                                         |           0   |       2.29 |
| The Seas with Nemo & Friends                                   |           0   |       2.5  |
| Turtle Talk With Crush                                         |           0   |       0    |
| Zootopia: Better Zoogether!                                    |           0   |       0.28 |
| Walt Disney's Enchanted Tiki Room                              |           0   |       1.67 |
| Walt Disney World Railroad - Main Street, U.S.A.               |           0   |       0    |
| Walt Disney World Railroad - Fantasyland                       |           0   |       0    |
| Vacation Fun - An Original Animated Short with Mickey & Minnie |           0   |       0    |
| Star Tours – The Adventures Continue                           |           0   |       0    |
| Tiana's Bayou Adventure                                        |           0   |       0    |
| The Hall of Presidents                                         |           0   |       0    |
| Swiss Family Treehouse                                         |           0   |       0    |
| The Barnstormer                                                |           2.5 |       3.93 |
| Mickey & Minnie's Runaway Railway                              |           2.5 |      16.29 |
| Prince Charming Regal Carrousel                                |           2.5 |       1.52 |
| "it's a small world"                                           |           2.5 |       6.29 |
| Spaceship Earth                                                |           2.5 |       7.05 |
| The Twilight Zone™ Tower of Terror                             |           2.5 |      14.64 |
| Tomorrowland Transit Authority PeopleMover                     |           5   |       8.33 |
| Test Track                                                     |           5   |      25    |
| Gran Fiesta Tour Starring The Three Caballeros                 |           5   |       1.67 |
| Living with the Land                                           |           5   |       1.67 |
| Millennium Falcon: Smugglers Run                               |           5   |       3.64 |
| Pirates of the Caribbean                                       |           5   |       3.33 |
| Haunted Mansion                                                |           5   |      15    |
| Buzz Lightyear’s Space Ranger Spin                             |           5   |      19.25 |
| Avatar Flight of Passage                                       |           5   |       9.72 |
| Enchanted Tales with Belle                                     |           5   |       1.67 |
| Na'vi River Journey                                            |           5   |       9.44 |
| Mickey's PhilharMagic                                          |           5   |       1.67 |
| Kilimanjaro Safaris                                            |           7.5 |       5    |
| Peter Pan's Flight                                             |           7.5 |       5.83 |
| Slinky Dog Dash                                                |           7.5 |       8.33 |
| Tomorrowland Speedway                                          |           7.5 |      12.78 |
| Space Mountain                                                 |          10   |       6.36 |
| Jungle Cruise                                                  |          10   |       0    |
| TRON Lightcycle / Run                                          |          10   |       7.5  |
| The Magic Carpets of Aladdin                                   |          10   |      10    |
| Rock ’n’ Roller Coaster Starring The Muppets                   |          10   |      30.91 |
| Toy Story Mania!                                               |          12.5 |      20    |
| Expedition Everest - Legend of the Forbidden Mountain          |          15   |       7.5  |
| Big Thunder Mountain Railroad                                  |          15   |       5    |
| Guardians of the Galaxy: Cosmic Rewind                         |          15   |      10.08 |
| Star Wars: Rise of the Resistance                              |          15   |      10.83 |
| Under the Sea - Journey of The Little Mermaid                  |          15   |      16.67 |
| The Many Adventures of Winnie the Pooh                         |          15   |      11.58 |
| Soarin' Across America                                         |          30   |      23.33 |

These are the numbers to beat. A model that can't beat persistence is a lesson, not a bullet.
