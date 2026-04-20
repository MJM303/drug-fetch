# Codex — GitHub Actions Drug Fetch

Runs `fetch_drug_data.py` across a large drug name list using GitHub Actions,
splitting the work into parallel chunks so the full 43k+ name list completes
in ~4 hours instead of 30+.

---

## Repo structure

```
.github/
  workflows/
    fetch_drug_data.yml   ← the workflow
scripts/
  split_names.py          ← splits name list into chunks (called by workflow)
  merge_results.py        ← merges chunk outputs into final CSVs (called by workflow)
fetch_drug_data.py        ← your fetch script (copy here from the project)
extract_drug_names.py     ← generates drug_names.txt (run locally first)
drug_names.txt            ← the name list (commit this to the repo)
requirements.txt
```

---

## One-time setup

### 1. Create the GitHub repo

```bash
git init codex-drug-fetch
cd codex-drug-fetch
# Copy all files above into this directory
git add .
git commit -m "initial setup"
gh repo create codex-drug-fetch --private --push --source=.
```

### 2. Add the openFDA API key (optional but recommended)

A free openFDA key raises the rate limit from 1,000 to 240,000 requests/day,
which matters when running thousands of drug lookups.

Get a free key at: https://open.fda.gov/apis/authentication/

Add it to your repo:
```
GitHub repo → Settings → Secrets and variables → Actions → New repository secret
Name:  OPENFDA_KEY
Value: your_key_here
```

If you skip this the workflow still works, just with lower FDA rate limits.

### 3. Generate and commit the name list

Run locally once to generate `drug_names.txt`:

```bash
python extract_drug_names.py \
  --source-dir ./Tables_for_database/ \
  --output drug_names.txt

git add drug_names.txt
git commit -m "add drug name list"
git push
```

---

## Running the workflow

Go to your GitHub repo → **Actions** → **Fetch Drug Data** → **Run workflow**

Fill in the inputs:

| Input | Description | Default |
|-------|-------------|---------|
| `countries` | ISO country codes to fetch | `US,UA,RU,FR` |
| `chunk_size` | Names per parallel job | `5000` |
| `name_file` | Path to name list in repo | `drug_names.txt` |
| `commit_results` | Commit merged CSVs back to repo | `no` |

Click **Run workflow**. 

---

## What happens

```
prepare job     (1 job)
  Splits drug_names.txt into N chunks of 5,000 names each
  Outputs a matrix so GitHub runs N jobs in parallel

fetch jobs      (N jobs, all running at the same time)
  Each job runs fetch_drug_data.py on its chunk
  Uploads its output CSVs as an artifact

merge job       (1 job, runs after all fetch jobs finish)
  Downloads all chunk artifacts
  Merges same-country CSVs and deduplicates
  Uploads final merged CSVs as a single artifact
  Prints a summary table in the workflow run page
```

With 43,479 names at 5,000 per chunk = **9 parallel jobs**.
All 9 run simultaneously. Total wall-clock time: **~4 hours**.

---

## Downloading results

After the workflow completes:

1. Go to the workflow run page
2. Scroll to the bottom — **Artifacts** section
3. Download `drug-data-<run_id>.zip`
4. Extract — you'll find one CSV per country ready for Codex upload

Or set `commit_results = yes` and the merged CSVs are automatically
committed to `data/drug-csvs/` in the repo.

---

## Re-running for new countries

Just run the workflow again with different country codes:

```
countries: MX,BR,DE,ES,IT,JP,KR
```

The workflow fetches only what you specify. You don't need to re-run
existing countries unless you want to refresh the data.

---

## Troubleshooting

**Some fetch jobs failed** — the merge job still runs on whatever succeeded.
Failed chunks are listed in the workflow summary. Re-run just the failed
jobs using GitHub's "Re-run failed jobs" button.

**Rate limited by Wikidata** — Wikidata occasionally throttles requests.
The script has retry logic but very large runs can still hit limits.
Reduce `chunk_size` to 2,000 for a slower but more reliable run.

**PubChem not finding a name** — appears in `lookup_summary.csv` as
"PubChem lookup failed". These are usually obscure research compounds
or names that need exact spelling. Not an error — just no data available.
