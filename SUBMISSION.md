# My dataset submission

## Inputs and outputs

**Inputs** (read-only, never changed):
- `training.to6_challenge3_inputs.returns_initial`: first delivery (rejected).
- `training.to6_challenge3_inputs.returns_corrected`: replacement delivery (accepted, used for the final run).
- Lineage view `training.to6_challenge3_inputs.source_lineage`: both deliveries come from `training.to6_challenge3_inputs.parsed_source`.

**What one row represents:** one synthetic tax return. `return_id` identifies the return and must be unique.
`taxpayer_id` identifies the filer, who can have returns in several years.

**Outputs** (all in my assigned schema `training.to6_c3_85231ee3fe58`):
- `initial_rejected_records`: the affected records from the initial batch, with the rule and reason.
- `return_features`: `return_id`, `tax_year`, `deduction_to_income`. One row per accepted return (12 rows).
- `features_train` (tax_year 2022, 4 rows), `features_validation` (2023, 4 rows), `features_test` (2024, 4 rows).

**Ratio definition:** `deduction_to_income = deduction / income`. When income is 0, the return stays in the
table and the ratio is NULL. `taxpayer_id`, `income` and `deduction` are not in the outputs (need-to-know rule).

## Checks and decisions

**Source references:** Catalog table descriptions (`information_schema.tables`), column metadata
(`information_schema.columns`), 5 sample rows and the full batch of `returns_initial`, and the `source_lineage` view.
Full-batch findings: 13 rows and 12 distinct return IDs (5 rows in 2022, 4 in 2023, 4 in 2024), one missing
income (R02), one repeated ID (R01) and two zero-income returns (R03, R09).

**Rules applied to both batches with the same code:**
- Completeness: `return_id`, `tax_year`, `income` and `deduction` must not be NULL (zero is valid).
- Uniqueness: each `return_id` must occur exactly once.

**Initial batch: REJECTED** before any feature output was written (`outputs/batch_assessment.json`).

| rule | return_id | taxpayer_id | tax_year | income | deduction | reason |
|---|---|---|---|---|---|---|
| completeness | R02 | F1 | 2022 | NULL | 33 | required value `income` is NULL |
| uniqueness | R01 | F0 | 2022 | 100 | 20 | `R01` occurs 2 times; must occur exactly once |
| uniqueness | R01 | F0 | 2022 | 100 | 20 | `R01` occurs 2 times; must occur exactly once |

The batch was rejected, not repaired. R02's unknown income was not set to 0, and neither R01 copy was dropped.
The affected records are kept in the table `initial_rejected_records` and in `outputs/initial_rejected_records.csv`.

**Replacement batch: ACCEPTED.** `returns_corrected` has 12 rows, 12 distinct return IDs, 0 completeness and
0 uniqueness violations. The notebook gate only sets it as the accepted table after it passes, and the
feature step refuses to read the initial batch.

**Feature table readback** (after saving, read back from `return_features`):
- The columns are exactly `return_id`, `tax_year`, `deduction_to_income`.
- The IDs and years match the accepted source one-to-one: 12 rows, 12 distinct IDs, none missing or extra.
- Nonzero expected value derived from the source: R01 has deduction 20 and income 100, so 20 / 100 = 0.2. Saved value: 0.2.
- Zero income: R03 (income 0, deduction 46) and R09 (income 0, deduction 124) are present with a NULL ratio,
  and they are the only NULL ratios.

## Two successful runs

Both were interactive notebook runs, so there are no Jobs run IDs.

| run | execution timestamp (UTC) | notebook URL | snapshot |
|---|---|---|---|
| 1 | 2026-11-20T17:15:12Z | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866560362 | `outputs/runs/run_20261120T171512Z.csv` / `.json` |
| 2 | 2026-11-20T17:16:33Z | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866560362 | `outputs/runs/run_20261120T171633Z.csv` / `.json` |

Both runs were made from the submitted notebook (`notebook`, saved as `notebook.py`).
Two earlier development runs of the same code, made before the notebook was moved to the repo root
(`run_20261120T170134Z` and `run_20261120T170530Z`), are also kept. All four snapshots are identical.

Each snapshot holds every return's `split_table`, `return_id`, `tax_year` and `deduction_to_income`, read from
the saved split tables. `outputs/run_comparison.json` compares the two snapshots row by row and reports
`identical_ids_and_values: true`, `no_appended_copies: true`, 12 rows in each run and no differences.

**How the handoff was checked (in every run):**
- Year assignment: every row in `features_train`, `features_validation` and `features_test` has that table's
  year (2022, 2023, 2024). The save step stops if any return has a year with no split table.
- One copy of every accepted return: no `return_id` is in two splits or twice in one split, and the three splits
  together equal `return_features` exactly.
- No appended copies: all tables are written with overwrite. In every output table's Delta history, each run
  (including the two development runs) is a single `CREATE OR REPLACE TABLE AS SELECT` with no appends, and after
  the final run the row counts are still 3 / 12 / 4 / 4 / 4.

## Final files and limitations

**Final exports under `outputs/`:**
- `initial_rejected_records.csv`: affected initial records with rule and reason.
- `batch_assessment.json`: results of both batches under the same rules, and the accepted table.
- `return_features.csv`, `features_train.csv`, `features_validation.csv`, `features_test.csv`: the saved tables.
- `runs/run_20261120T171512Z.csv` + `.json` and `runs/run_20261120T171633Z.csv` + `.json`: the two final runs' IDs, values and run references.
- `runs/run_20261120T170134Z.*` and `runs/run_20261120T170530Z.*`: the earlier development runs (identical results).
- `run_comparison.json`: comparison of the two final runs.

**Practice file (not part of the task):** `outputs/connection-practice.json`.

**Not verified / limitations:**
- The Catalog Explorer Lineage tab was not checked. The upstream source comes from the supplied `source_lineage` view.
- Nonnegative amounts were not enforced as a separate rule. No negative values were seen in the data.
- In `initial_rejected_records.csv`, income shows as `100.0` because pandas uses floats when a column has a NULL.
  The saved table keeps the original INT values.
- These checks are this task's data contract only. They do not show that model leakage is eliminated. Labels and
  information available at prediction time must be assessed in later modelling work.
- The final commit SHA is reported in Classroom, not here.
