# ML Projects Roadmap

*Scope discipline: each phase is roughly one weekend session with a hard
"done" definition. No phase starts until the prior one's done-check passes,
and no session starts in a week where zero applications went out. Bullets
enter master_resume.yaml only when a phase is DONE - the bank's truth rule
applies to future work too.*

---

## Project 1 - Wait-Time Prediction (theme-park-pipeline repo)

**The pitch:** the pipeline already ingests hourly wait times into BigQuery
with dbt models on top. Adding prediction turns it into an end-to-end ML
system - ingestion -> warehouse -> features -> model -> served predictions ->
live monitoring - built on months of data I collected myself. This is the
exact architecture ML-adjacent DE interviews probe.

**Target claim (only after Phase 4):** "Extended a production data pipeline
with an ML layer predicting next-hour ride wait times - engineered lagged
time-series features in dbt, trained gradient-boosted models with
walk-forward validation, and deployed hourly batch predictions with live
accuracy monitoring in the dashboard."

### Phase 0 - Data audit (half a session)
- [ ] Row counts: total snapshots, per attraction, date range
- [ ] Gap analysis: missing hours (Actions failures, the Snowflake->BigQuery
      migration seam), park closures vs. true gaps
- [ ] Target definition: predict standby wait at T+1h, per attraction
- [ ] Decide inclusion rules (attractions with sparse data, closed rides)
- **Done when:** a short DATA_AUDIT.md states usable row count, known gaps,
  and the target definition. If usable history < ~2 months, pause project
  and let the pipeline keep collecting.

### Phase 1 - Baselines (half a session)
- [ ] Naive persistence: predict T+1h = current wait
- [ ] Seasonal naive: predict T+1h = same hour last week (or hour-of-day mean)
- [ ] Metric: MAE in minutes, per attraction and overall, on a held-out
      final-N-weeks window
- **Done when:** baseline MAE numbers exist in a notebook/README table.
  These are the numbers to beat - a model that can't beat persistence is
  a lesson, not a bullet. (This phase IS interview material by itself:
  "I established naive baselines before training anything.")

### Phase 2 - Feature layer (one session)
- [ ] dbt feature mart: lags (1h, 2h, 24h, 168h), rolling means/max (3h,
      24h), hour-of-day, day-of-week, park, is-weekend
- [ ] Optional: school-holiday/park-hours flag if cheaply available
- [ ] dbt tests on the feature mart (no leakage: features at time T use only
      data <= T)
- **Done when:** feature mart builds green with tests; one query returns a
  model-ready training frame.

### Phase 3 - Model (one session)
- [ ] Gradient boosting (XGBoost or LightGBM) on the feature frame
- [ ] Walk-forward validation - time-ordered splits ONLY (random splits =
      leakage = the classic junior mistake; not making it is the story)
- [ ] Compare vs. Phase 1 baselines; simple feature-importance readout
- **Done when:** model beats seasonal-naive MAE by a margin you can state
  honestly, with the comparison table in the repo.

### Phase 3b - Databricks port (optional, one session)
- [ ] Re-run feature build + training on Databricks Community Edition
      (PySpark for the feature job)
- **Why:** closes the Spark/Databricks gap (34% of 2026 target JDs) with a
  truthful "used" claim. Optional because Phase 4 must not wait on it.

### Phase 4 - Serve + monitor (one session)
- [ ] Hourly batch predictions via the existing GitHub Actions schedule,
      written to a predictions table in BigQuery
- [ ] Dashboard: predicted vs. actual overlay + rolling MAE panel (live
      model monitoring - the MLOps-adjacent credibility piece)
- **Done when:** the public dashboard shows tomorrow's predictions and
  yesterday's accuracy, unattended.

### Phase 5 - Write-up (half a session)
- [ ] README section: architecture diagram, baseline-vs-model table, honest
      limitations (data gaps, cold-start rides, holiday drift)
- [ ] Bullets into master_resume.yaml (now claimable)

---

## Project 2 - JD Triage Agent (job-hunt repo)

**The pitch:** an LLM (Haiku - cheap, fast) reads each new posting after the
daily run and emits a structured verdict; a Fit column lands in jobs.html.
Upgrades the suite's story to "LLM-in-the-loop pipeline" - the entry ticket
for AI Engineer conversations - and makes every morning's triage faster.

### Phase 1 - Core (one session)
- [ ] `triage.py`: for each new posting with a saved JD, one Haiku call ->
      strict JSON: {{seniority_fit, remote_truth, ghost_risk, one_liner}}
- [ ] Cache verdicts in SQLite keyed by posting uid (never re-triage)
- [ ] Env-var API key via the existing .env loader
- **Done when:** `python triage.py` verdicts a day's new postings for
  pennies and survives malformed JSON gracefully.

### Phase 2 - Report integration (half a session)
- [ ] Fit column + one-liner tooltip in jobs.html; sortable, filterable
      with the existing bar
- **Done when:** the morning report shows verdicts inline.

### Phase 3 - Calibrate (ongoing, zero sessions)
- [ ] When a verdict disagrees with your read, note it; adjust the prompt
      monthly. The disagreement log is itself portfolio material
      (LLM evaluation in the wild).

---

## Sequencing

1. **Triage agent Phase 1-2 first** - one weekend, pays rent immediately
   every morning, smallest scope.
2. **Wait-time Phases 0-1 next** - the audit may reveal you should wait for
   more data, and baselines are cheap. Better to know early.
3. Then wait-time 2 -> 3 -> 4, one weekend each. 3b opportunistic.
4. Nothing here outranks applications. The projects exist to strengthen
   applications; a week with zero submissions is a week these stay frozen.
