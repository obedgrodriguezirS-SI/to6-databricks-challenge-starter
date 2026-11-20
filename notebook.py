# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.
# MAGIC
# MAGIC In this part I just want to get to know the data before working with it. I want to answer three simple questions:
# MAGIC What is one row? Where does the data come from? Is anything wrong with it?
# MAGIC
# MAGIC To find out, I looked at:
# MAGIC - What the Catalog says about the tables (their descriptions).
# MAGIC - The lineage view the training team gave us, which says where the data comes from.
# MAGIC - Not the lineage graph in Catalog Explorer: I did not check it. Where the data comes from is taken from the supplied `source_lineage` view.
# MAGIC - A few real rows first, and then all the rows.
# MAGIC
# MAGIC All the input tables are in `training.to6_challenge3_inputs`. Here I only look at `returns_initial` (the first batch).
# MAGIC The second batch, `returns_corrected`, is checked in section 2.
# MAGIC The results below cover all 13 rows of `returns_initial`, not just a sample.

# COMMAND ----------

# MAGIC %md
# MAGIC Each table in the Catalog can have a short description. I read them first because they explain what each table is for.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, table_type, comment
# MAGIC FROM training.information_schema.tables
# MAGIC WHERE table_schema = 'to6_challenge3_inputs'
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC
# MAGIC | table_name | comment |
# MAGIC |---|---|
# MAGIC | parsed_source | Synthetic parsed source: one row is one return; return_id identifies the return, taxpayer_id identifies a filer who may recur across years. Nonnegative amounts; zero income is valid. |
# MAGIC | returns_corrected | Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness. |
# MAGIC | returns_initial | Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output. |
# MAGIC
# MAGIC **What this tells me:**
# MAGIC - `returns_initial` is the first delivery, and its own description already warns that it has mistakes.
# MAGIC - `returns_corrected` is the fixed version.
# MAGIC - Both were made from the same table, `parsed_source`.
# MAGIC - `parsed_source` has the most useful description: one row is one return, `return_id` is the return and `taxpayer_id` is the person.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ordinal_position, column_name, data_type, is_nullable, comment
# MAGIC FROM training.information_schema.columns
# MAGIC WHERE table_schema = 'to6_challenge3_inputs' AND table_name = 'returns_initial'
# MAGIC ORDER BY ordinal_position

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC
# MAGIC | # | column_name | data_type | comment |
# MAGIC |---|---|---|---|
# MAGIC | 1 | return_id | STRING | none |
# MAGIC | 2 | taxpayer_id | STRING | none |
# MAGIC | 3 | tax_year | INT | none |
# MAGIC | 4 | income | INT | none |
# MAGIC | 5 | deduction | INT | none |
# MAGIC
# MAGIC **What this tells me:** the columns don't have their own descriptions. So to know what each column means,
# MAGIC I used the `parsed_source` table description.
# MAGIC Also, the table allows empty values in any column, so the table itself won't stop bad data from coming in.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM training.to6_challenge3_inputs.source_lineage

# COMMAND ----------

# MAGIC %md
# MAGIC Now I look at 5 real rows to see what the data looks like. The task asks for at least 3.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM training.to6_challenge3_inputs.returns_initial
# MAGIC ORDER BY return_id
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC
# MAGIC | return_id | taxpayer_id | tax_year | income | deduction |
# MAGIC |---|---|---|---|---|
# MAGIC | R01 | F0 | 2022 | 100 | 20 |
# MAGIC | R01 | F0 | 2022 | 100 | 20 |
# MAGIC | R02 | F1 | 2022 | NULL | 33 |
# MAGIC | R03 | F2 | 2022 | 0 | 46 |
# MAGIC | R04 | F3 | 2022 | 400 | 59 |
# MAGIC
# MAGIC Just in these 5 rows I can already see three things: `R01` is there twice, `R02` has no income, and `R03` has an income of 0.
# MAGIC
# MAGIC **What I now understand**
# MAGIC - **One row = one tax return for one year.** The data is made up (synthetic), not real people.
# MAGIC - **The columns:**
# MAGIC   - `return_id`: the name of the return, like `R01`.
# MAGIC   - `taxpayer_id`: the person who filed it, like `F0`. One person can file every year.
# MAGIC     For example, `F0` filed `R01` in 2022, `R05` in 2023 and `R09` in 2024. So it is normal for this one to repeat.
# MAGIC   - `tax_year`: the year of the return.
# MAGIC   - `income`: how much the person earned. It can be 0, but never negative.
# MAGIC   - `deduction`: how much they deduct. Never negative.
# MAGIC - **Columns that can never be empty:** `return_id`, `tax_year`, `income` and `deduction`.
# MAGIC - **The ID that tells returns apart:** `return_id`. Every return should be there only once.
# MAGIC   `taxpayer_id` doesn't work for this, because one person can have several returns.
# MAGIC - **Where the data comes from:** `training.to6_challenge3_inputs.parsed_source`.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. How many returns are there each year?
# MAGIC 2. Is any required value empty?
# MAGIC 3. Is any `return_id` repeated?
# MAGIC 4. Which returns have an income of 0?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Returns by year (a NULL tax_year would show up as its own group)
# MAGIC SELECT tax_year, COUNT(*) AS row_count, COUNT(DISTINCT return_id) AS distinct_return_ids
# MAGIC FROM training.to6_challenge3_inputs.returns_initial
# MAGIC GROUP BY tax_year
# MAGIC ORDER BY tax_year

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC
# MAGIC | tax_year | row_count | distinct_return_ids |
# MAGIC |---|---|---|
# MAGIC | 2022 | 5 | 4 |
# MAGIC | 2023 | 4 | 4 |
# MAGIC | 2024 | 4 | 4 |
# MAGIC
# MAGIC There are 13 rows but only 12 different returns. The extra row is in 2022.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Missing required values
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT_IF(return_id IS NULL) AS missing_return_id,
# MAGIC   COUNT_IF(tax_year IS NULL) AS missing_tax_year,
# MAGIC   COUNT_IF(income IS NULL) AS missing_income,
# MAGIC   COUNT_IF(deduction IS NULL) AS missing_deduction
# MAGIC FROM training.to6_challenge3_inputs.returns_initial

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC
# MAGIC | total_rows | missing_return_id | missing_tax_year | missing_income | missing_deduction |
# MAGIC |---|---|---|---|---|
# MAGIC | 13 | 0 | 0 | 1 | 0 |
# MAGIC
# MAGIC Only one value is missing: `R02` (person `F1`, 2022) has no income.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicate return IDs
# MAGIC SELECT return_id, COUNT(*) AS occurrences
# MAGIC FROM training.to6_challenge3_inputs.returns_initial
# MAGIC WHERE return_id IS NOT NULL
# MAGIC GROUP BY return_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY return_id

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC
# MAGIC | return_id | occurrences |
# MAGIC |---|---|
# MAGIC | R01 | 2 |
# MAGIC
# MAGIC `R01` is there twice, and both rows are exactly the same (`F0`, 2022, income 100, deduction 20).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Zero-income returns
# MAGIC SELECT *
# MAGIC FROM training.to6_challenge3_inputs.returns_initial
# MAGIC WHERE income = 0
# MAGIC ORDER BY return_id

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC
# MAGIC | return_id | taxpayer_id | tax_year | income | deduction |
# MAGIC |---|---|---|---|---|
# MAGIC | R03 | F2 | 2022 | 0 | 46 |
# MAGIC | R09 | F0 | 2024 | 0 | 124 |
# MAGIC
# MAGIC Two returns have an income of 0: `R03` and `R09`.

# COMMAND ----------

# DBTITLE 1,What the findings mean for the next steps
# MAGIC %md
# MAGIC
# MAGIC **1. Returns per year**
# MAGIC - What I saw: 5 rows in 2022, 4 in 2023 and 4 in 2024. All years are between 2022 and 2024, and none is empty.
# MAGIC - Why it matters: in section 4 each year becomes its own table (2022 for training, 2023 for validation, 2024 for test).
# MAGIC   The good news is that every year has data. But 2022 has one row too many because of the repeated `R01`.
# MAGIC   
# MAGIC **2. Missing values**
# MAGIC - What I saw: one missing value. `R02` (person `F1`, 2022) has no income (NULL). No other required value is empty.
# MAGIC - Why it matters: `income` is a required field, so `R02` breaks the completeness rule and section 2 must reject this batch.
# MAGIC   The income is unknown, so it must not be replaced with 0. That would invent a value and make `R02` look like a real zero-income return.
# MAGIC
# MAGIC **3. Repeated IDs**
# MAGIC - What I saw: `R01` is there twice, and both rows are exactly the same (`F0`, 2022, income 100, deduction 20).
# MAGIC - Why it matters: each return must occur exactly once, so `R01` breaks the uniqueness rule and section 2 must reject this batch.
# MAGIC   Quietly deleting one copy is not allowed, because it would hide the delivery error. Both copies are kept as evidence.
# MAGIC
# MAGIC **4. Income of 0**
# MAGIC - What I saw: two returns, `R03` (2022) and `R09` (2024), have an income of 0.
# MAGIC - Why it matters: zero income is valid, so these are not errors and must not be rejected.
# MAGIC   In section 3 they stay in `return_features`, but their `deduction_to_income` is NULL, because you cannot divide by 0.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,Quality rules: completeness and uniqueness
from pyspark.sql import functions as F, Window

# Data contract from CHALLENGE.md: these fields are required, and each return_id must occur exactly once.
REQUIRED_COLUMNS = ["return_id", "tax_year", "income", "deduction"]
KEY_COLUMN = "return_id"


def find_violations(table_name):
    """One row per (record, failed rule). Records are kept exactly as delivered: nothing is changed, filled or dropped."""
    df = spark.table(table_name)

    # Completeness: every required field must have a value (zero is a value; NULL is not).
    completeness = None
    for col in REQUIRED_COLUMNS:
        failed = (df.filter(F.col(col).isNull())
                    .withColumn("rule", F.lit("completeness"))
                    .withColumn("reason", F.lit(f"required value '{col}' is NULL")))
        completeness = failed if completeness is None else completeness.unionByName(failed)

    # Uniqueness: a return_id may appear only once. Every copy is kept as evidence, not just the extras.
    uniqueness = (df.filter(F.col(KEY_COLUMN).isNotNull())
                    .withColumn("occurrences", F.count(F.lit(1)).over(Window.partitionBy(KEY_COLUMN)))
                    .filter(F.col("occurrences") > 1)
                    .withColumn("rule", F.lit("uniqueness"))
                    .withColumn("reason", F.concat(F.lit("return_id '"), F.col(KEY_COLUMN), F.lit("' occurs "),
                                                   F.col("occurrences").cast("string"), F.lit(" times; must occur exactly once")))
                    .drop("occurrences"))

    return (completeness.unionByName(uniqueness)
                        .withColumn("source_table", F.lit(table_name))
                        .select("source_table", "rule", "reason", *df.columns))


def assess_batch(table_name):
    """Apply the same rules to any batch and return (summary, violating records)."""
    df = spark.table(table_name)
    violations = find_violations(table_name)
    by_rule = {r["rule"]: r["n"] for r in violations.groupBy("rule").agg(F.count("*").alias("n")).collect()}
    summary = {
        "table": table_name,
        "total_rows": df.count(),
        "distinct_return_ids": df.select(KEY_COLUMN).distinct().count(),
        "completeness_violations": by_rule.get("completeness", 0),
        "uniqueness_violations": by_rule.get("uniqueness", 0),
    }
    summary["decision"] = "ACCEPTED" if not by_rule else "REJECTED"
    return summary, violations

# COMMAND ----------

# DBTITLE 1,Assess the initial batch
initial_summary, initial_violations = assess_batch(INITIAL_TABLE)
print(initial_summary)
display(initial_violations.orderBy("rule", "return_id"))

# COMMAND ----------

# DBTITLE 1,Assess the corrected batch with the same rules
corrected_summary, corrected_violations = assess_batch(CORRECTED_TABLE)
print(corrected_summary)
display(corrected_violations.orderBy("rule", "return_id"))

# COMMAND ----------

# DBTITLE 1,Gate: only an accepted batch may feed the feature output
# The gate runs before any feature output is written (section 3 reads only ACCEPTED_TABLE).
# A rejected batch is never repaired here: no dropping duplicates, no filling NULL income with 0.
assert initial_summary["decision"] == "REJECTED", "Expected the initial batch to fail; re-check the rules"

if corrected_summary["decision"] != "ACCEPTED":
    raise ValueError(f"Replacement batch also failed the checks; stopping before feature output: {corrected_summary}")

ACCEPTED_TABLE = CORRECTED_TABLE
print({"rejected": INITIAL_TABLE, "accepted": ACCEPTED_TABLE})

# COMMAND ----------

# DBTITLE 1,Save rejection evidence (table + outputs/ exports)
from pathlib import Path
import json
from datetime import datetime, timezone

repo_root = Path("/Workspace/Users/obed.g.rodriguez.irs@catalyte.ai/to6-databricks-challenge-starter")
(repo_root / "outputs").mkdir(exist_ok=True)

# Affected initial records with the rule and reason, kept as delivered. Overwrite so reruns do not append copies.
(initial_violations.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{OUTPUT_SCHEMA}.initial_rejected_records"))

(initial_violations.orderBy("rule", "return_id").toPandas()
    .to_csv(repo_root / "outputs/initial_rejected_records.csv", index=False))

assessment = {
    "assessed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "rules": {"completeness": f"{REQUIRED_COLUMNS} must not be NULL",
              "uniqueness": f"{KEY_COLUMN} must occur exactly once"},
    "batches": [initial_summary, corrected_summary],
    "accepted_table": ACCEPTED_TABLE,
}
(repo_root / "outputs/batch_assessment.json").write_text(json.dumps(assessment, indent=2) + "\n", encoding="utf-8")

# Read back what was saved
display(spark.table(f"{OUTPUT_SCHEMA}.initial_rejected_records").orderBy("rule", "return_id"))

# COMMAND ----------

# DBTITLE 1,Why the initial batch was rejected and what happens next
# MAGIC %md
# MAGIC **Decision: `returns_initial` is REJECTED; `returns_corrected` is ACCEPTED and used for the final run.**
# MAGIC
# MAGIC Both batches were checked with the same two rules (`assess_batch` above):
# MAGIC - **Completeness:** `return_id`, `tax_year`, `income` and `deduction` must not be NULL. Zero is a valid value.
# MAGIC - **Uniqueness:** each `return_id` must occur exactly once.
# MAGIC
# MAGIC **What failed in `returns_initial` (13 rows, 12 distinct return IDs):**
# MAGIC
# MAGIC | rule | return_id | taxpayer_id | tax_year | income | deduction | reason |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | completeness | R02 | F1 | 2022 | NULL | 33 | required value `income` is NULL |
# MAGIC | uniqueness | R01 | F0 | 2022 | 100 | 20 | `R01` occurs 2 times; must occur exactly once |
# MAGIC | uniqueness | R01 | F0 | 2022 | 100 | 20 | `R01` occurs 2 times; must occur exactly once |
# MAGIC
# MAGIC - R02's income is unknown, so its ratio cannot be calculated. Putting 0 there would invent a value and make it look like a valid zero-income return.
# MAGIC - R01 is there twice. Quietly removing one copy would hide a delivery error.
# MAGIC - So the whole batch is rejected rather than repaired. These 3 records and their reasons are saved in `initial_rejected_records` and `outputs/initial_rejected_records.csv`.
# MAGIC - R03 and R09 have income 0. That is allowed, so they are not violations.
# MAGIC
# MAGIC **`returns_corrected` (12 rows, 12 distinct return IDs):** the same rules found 0 completeness and 0 uniqueness violations.
# MAGIC In this batch R02 has income 200, which comes from the data owner's replacement, not from us. R01 occurs once.
# MAGIC
# MAGIC **Next action:** section 3 builds `return_features` only from `ACCEPTED_TABLE` (= `returns_corrected`). The initial batch is never used for feature output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Build and save return_features from the accepted batch
from pyspark.sql import functions as F

FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"

# Gate from section 2: feature output is only written from a batch that passed both checks.
assert "ACCEPTED_TABLE" in globals(), "Run section 2 first: no batch has been accepted"
assert ACCEPTED_TABLE != INITIAL_TABLE, "The rejected initial batch must never feed feature output"
assert assess_batch(ACCEPTED_TABLE)[0]["decision"] == "ACCEPTED", "Accepted batch no longer passes the checks"

# deduction_to_income = deduction / income. Zero income is valid: the return stays, with a NULL ratio.
# taxpayer_id and the raw amounts are left out (need-to-know rule).
return_features = (spark.table(ACCEPTED_TABLE)
    .select(
        "return_id",
        "tax_year",
        F.when(F.col("income") != 0, F.col("deduction") / F.col("income"))
         .cast("double").alias("deduction_to_income"),
    ))

# Overwrite (not append) so a second full run leaves the same rows, without copies.
(return_features.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(FEATURES_TABLE))
print(f"Saved {FEATURES_TABLE} from {ACCEPTED_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read back return_features and verify the data contract
saved = spark.table(FEATURES_TABLE)
source = spark.table(ACCEPTED_TABLE)

# 1. Exactly the three agreed columns (no taxpayer_id, no raw amounts).
assert saved.columns == ["return_id", "tax_year", "deduction_to_income"], saved.columns

# 2. Every accepted return exactly once: same IDs and years as the source, no extras, no copies.
assert saved.count() == source.count() == saved.select("return_id").distinct().count()
assert saved.select("return_id", "tax_year").exceptAll(source.select("return_id", "tax_year")).count() == 0
assert source.select("return_id", "tax_year").exceptAll(saved.select("return_id", "tax_year")).count() == 0

saved_by_id = {r["return_id"]: r for r in saved.collect()}

# 3. Nonzero case: expected value derived by hand from the source amounts, compared with the saved table.
nonzero = source.filter("income > 0").orderBy("return_id").first()
expected = nonzero["deduction"] / nonzero["income"]
actual = saved_by_id[nonzero["return_id"]]["deduction_to_income"]
assert abs(actual - expected) < 1e-12, (nonzero["return_id"], actual, expected)
print(f"Nonzero check: {nonzero['return_id']} = {nonzero['deduction']} / {nonzero['income']} = {expected}; saved {actual}")

# 4. Zero-income cases: still present, ratio NULL.
zero_ids = [r["return_id"] for r in source.filter("income = 0").collect()]
assert zero_ids, "Expected at least one zero-income return to check"
for rid in zero_ids:
    assert rid in saved_by_id and saved_by_id[rid]["deduction_to_income"] is None, rid
print(f"Zero-income check: {zero_ids} present with NULL ratio")

# 5. The only NULL ratios are the zero-income returns.
assert sorted(r["return_id"] for r in saved.filter("deduction_to_income IS NULL").collect()) == sorted(zero_ids)

display(saved.orderBy("return_id"))

# COMMAND ----------

# DBTITLE 1,What return_features contains and how it was checked
# MAGIC %md
# MAGIC **`return_features`** is built only from `ACCEPTED_TABLE` (`returns_corrected`), after the section 2 gate. The rejected `returns_initial` is never used.
# MAGIC
# MAGIC - **Columns:** exactly `return_id`, `tax_year`, `deduction_to_income`. `taxpayer_id`, `income` and `deduction` are left out (need-to-know rule).
# MAGIC - **Ratio:** `deduction_to_income = deduction / income`. When income is 0 the return stays in the table and the ratio is NULL (dividing by zero has no meaning). The ratio is not set to 0.
# MAGIC - **Rows:** one per accepted return, 12 in total. The table is written with overwrite, so running again replaces the rows instead of adding copies.
# MAGIC
# MAGIC **Checks after reading the saved table back:**
# MAGIC - The columns are exactly the three above.
# MAGIC - The IDs and years match the source one-to-one, with no missing or extra rows and no duplicates.
# MAGIC - **Nonzero case:** from the source, R01 has deduction 20 and income 100, so the expected ratio is 20 / 100 = 0.2. The saved value must match.
# MAGIC - **Zero-income cases:** R03 (income 0, deduction 46) and R09 (income 0, deduction 124) are in the table with a NULL ratio. These are the only NULL ratios.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,Save the year split tables from return_features
from pyspark.sql import functions as F

# Each year has exactly one split table. Splits are built from the SAVED feature table, not from the inputs.
SPLITS = {2022: "features_train", 2023: "features_validation", 2024: "features_test"}
SPLIT_TABLES = {year: f"{OUTPUT_SCHEMA}.{name}" for year, name in SPLITS.items()}

features = spark.table(FEATURES_TABLE)

# Stop if any return has a year with no assigned split: it would silently drop out of the handoff.
years_present = {r["tax_year"] for r in features.select("tax_year").distinct().collect()}
unassigned = years_present - set(SPLITS)
assert not unassigned, f"Returns with a tax_year that has no split table: {sorted(unassigned, key=str)}"

# Overwrite (not append) so a second full run leaves the same rows, without copies.
for year, table in SPLIT_TABLES.items():
    (features.filter(F.col("tax_year") == year)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(table))
    print(f"Saved {table} (tax_year = {year})")

# COMMAND ----------

# DBTITLE 1,Read back the split tables and verify year assignment
from functools import reduce

features = spark.table(FEATURES_TABLE)
split_dfs = {year: spark.table(table) for year, table in SPLIT_TABLES.items()}

for year, df in split_dfs.items():
    # Same three columns as return_features.
    assert df.columns == features.columns, (SPLIT_TABLES[year], df.columns)
    # Every row is in its assigned year.
    wrong_year = df.filter(F.col("tax_year") != year).count()
    assert wrong_year == 0, f"{SPLIT_TABLES[year]} has {wrong_year} rows outside {year}"
    # No return appears twice inside one split.
    assert df.count() == df.select("return_id").distinct().count(), f"Duplicate return_id in {SPLIT_TABLES[year]}"

all_splits = reduce(lambda a, b: a.unionByName(b), split_dfs.values())

# Every accepted return is in exactly one split: no return in two splits, none missing, none extra.
assert all_splits.count() == all_splits.select("return_id").distinct().count(), "A return_id is in more than one split"
assert all_splits.exceptAll(features).count() == 0, "Split tables contain rows that are not in return_features"
assert features.exceptAll(all_splits).count() == 0, "Some return_features rows are missing from the split tables"

display(spark.createDataFrame(
    [(SPLIT_TABLES[y], y, df.count()) for y, df in split_dfs.items()] + [(FEATURES_TABLE, None, features.count())],
    "table string, tax_year int, rows long"))

# COMMAND ----------

# DBTITLE 1,Snapshot this run's IDs and values, and export the final tables
from pathlib import Path
from datetime import datetime, timezone
import json

repo_root = Path("/Workspace/Users/obed.g.rodriguez.irs@catalyte.ai/to6-databricks-challenge-starter")
runs_dir = repo_root / "outputs/runs"
runs_dir.mkdir(parents=True, exist_ok=True)

# Run reference: execution timestamp and notebook URL (taken from the notebook context when available).
run_at = datetime.now(timezone.utc)
run_id = run_at.strftime("%Y%m%dT%H%M%SZ")
try:
    ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())["attributes"]
    notebook_path = ctx.get("notebook_path")
    notebook_url = f"https://{ctx['browserHostName']}/editor/notebooks/{ctx['notebook_id']}" if ctx.get("browserHostName") and ctx.get("notebook_id") else None
except Exception as e:
    notebook_path, notebook_url = None, None
    print(f"Could not read the notebook context ({e}); add the notebook URL to SUBMISSION.md by hand.")

# IDs and values of this run, taken from the saved split tables (one row per return, with its split).
snapshot = (reduce(lambda a, b: a.unionByName(b),
                   [spark.table(t).withColumn("split_table", F.lit(t.split(".")[-1])) for t in SPLIT_TABLES.values()])
            .select("split_table", "return_id", "tax_year", "deduction_to_income")
            .orderBy("return_id")
            .toPandas())
snapshot.to_csv(runs_dir / f"run_{run_id}.csv", index=False)
(runs_dir / f"run_{run_id}.json").write_text(json.dumps({
    "run_id": run_id,
    "executed_at_utc": run_at.isoformat(timespec="seconds"),
    "notebook_path": notebook_path,
    "notebook_url": notebook_url,
    "accepted_table": ACCEPTED_TABLE,
    "rows": len(snapshot),
    "distinct_return_ids": int(snapshot["return_id"].nunique()),
}, indent=2) + "\n", encoding="utf-8")

# Small final exports of the saved tables (overwritten each run).
for table in [FEATURES_TABLE, *SPLIT_TABLES.values()]:
    (spark.table(table).orderBy("return_id").toPandas()
        .to_csv(repo_root / f"outputs/{table.split('.')[-1]}.csv", index=False))

print(f"Run {run_id}: snapshot of {len(snapshot)} rows saved; url={notebook_url}")

# COMMAND ----------

# DBTITLE 1,Compare the last two successful runs (IDs and values)
import pandas as pd

run_files = sorted(runs_dir.glob("run_*.csv"))
if len(run_files) < 2:
    print(f"{len(run_files)} run snapshot(s) so far. Run the whole notebook again for the second run, then this cell compares them.")
else:
    previous_file, latest_file = run_files[-2], run_files[-1]
    # float_precision="round_trip" reads ratios back exactly as saved (the default parser can drop the last digit).
    previous = pd.read_csv(previous_file, float_precision="round_trip").sort_values("return_id").reset_index(drop=True)
    latest = pd.read_csv(latest_file, float_precision="round_trip").sort_values("return_id").reset_index(drop=True)

    # Row-by-row comparison of split, return_id, tax_year and ratio (NULL ratios must be NULL in both runs).
    merged = previous.merge(latest, on="return_id", how="outer", suffixes=("_previous", "_latest"), indicator=True)
    differences = merged[
        (merged["_merge"] != "both")
        | (merged["split_table_previous"] != merged["split_table_latest"])
        | (merged["tax_year_previous"] != merged["tax_year_latest"])
        | ~((merged["deduction_to_income_previous"] == merged["deduction_to_income_latest"])
            | (merged["deduction_to_income_previous"].isna() & merged["deduction_to_income_latest"].isna()))
    ]
    identical = differences.empty and previous.equals(latest)
    no_copies = all(df["return_id"].is_unique for df in (previous, latest))

    comparison = {
        "previous_run": json.loads(previous_file.with_suffix(".json").read_text()),
        "latest_run": json.loads(latest_file.with_suffix(".json").read_text()),
        "rows_previous": len(previous),
        "rows_latest": len(latest),
        "no_appended_copies": bool(no_copies),
        "identical_ids_and_values": bool(identical),
        "differences": differences.astype(str).to_dict(orient="records"),
    }
    (repo_root / "outputs/run_comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")

    print({k: comparison[k] for k in ["rows_previous", "rows_latest", "no_appended_copies", "identical_ids_and_values"]})
    assert no_copies, "A run snapshot contains the same return_id twice"
    assert identical, f"Runs differ:\n{differences}"
    display(merged.drop(columns="_merge"))

# COMMAND ----------

# DBTITLE 1,How the year splits and the two-run evidence work
# MAGIC %md
# MAGIC **Year split tables** (all built from the saved `return_features`, same three columns):
# MAGIC
# MAGIC | table | tax_year | role |
# MAGIC |---|---|---|
# MAGIC | `features_train` | 2022 | training |
# MAGIC | `features_validation` | 2023 | validation |
# MAGIC | `features_test` | 2024 | test |
# MAGIC
# MAGIC **Checks after reading the split tables back:**
# MAGIC - Every row in a split table has that table's year.
# MAGIC - No `return_id` appears in two splits or twice in one split.
# MAGIC - The three splits together equal `return_features` exactly (no missing or extra rows). If a return had a year outside 2022–2024, the save cell stops instead of leaving it out.
# MAGIC - All tables are written with overwrite, so a rerun replaces rows instead of appending copies.
# MAGIC
# MAGIC **Two successful runs:** run the whole notebook top to bottom twice.
# MAGIC - Each run saves its IDs and values (`split_table`, `return_id`, `tax_year`, `deduction_to_income`) to `outputs/runs/run_<UTC time>.csv`. Its run reference (UTC execution time and notebook URL) goes to the matching `.json`.
# MAGIC - After the second run, the comparison cell matches the two snapshots row by row and writes `outputs/run_comparison.json`. The run is only accepted if both runs have the same return IDs, splits and values, and neither has a repeated `return_id`.
# MAGIC - Final exports: `outputs/return_features.csv`, `features_train.csv`, `features_validation.csv`, `features_test.csv`, plus `initial_rejected_records.csv` and `batch_assessment.json` from section 2.