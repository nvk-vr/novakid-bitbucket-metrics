# Bitbucket PR Metrics (Kids Area)

This script collects Pull Request metrics from Bitbucket Cloud across project repositories and computes sprint-level aggregates using `Thu 07:30 UTC` boundaries.

## Requirements

- Python `3.12.x`
- Bitbucket API token with permissions:
  - `Repositories: Read`
  - `Pull requests: Read`

Check Python version:

```bash
python3.12 --version
```

## Setup

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Auth

```bash
export BB_API_TOKEN="<your-api-token>"
export BB_USERNAME="<your-bitbucket-username>"
```

Alternative if your workspace uses email-based login:

```bash
export BB_API_TOKEN="<your-api-token>"
export BB_EMAIL="<your-atlassian-email>"
```

## Run

Example for your workspace/project:

```bash
python3.12 bb_pr_metrics.py \
  --workspace novakidschool \
  --project-uuid "{6cadc6e0-6e64-46fa-b8c1-aa6468d65783}" \
  --sprints 8 \
  --out ./out
```

Useful flags:

- `--no-diffstat` - faster, skips diffstat calls.
- `--no-include-total` - do not add `kids_area_total` rows to CSV.
- `--no-report-md` - do not generate markdown report.

## Outputs

After running, files in `./out`:

1. `pr_facts.csv` - raw PR-level facts.
2. `sprint_repo_metrics.csv` - metrics by `repo × sprint × dataset`.
3. `sprint_report.md` - human-readable report for Confluence/Slack.

Dataset values:

- `feature_all`
- `feature_no_chore_bump`
- `releases`
- `hotfix_strict`
- `hotfix_candidates`
- `other`

## Metric rules

- Sprint boundary: `Thu 07:30 UTC`, sprint length is 14 days.
- `first response`: any activity not made by the PR author and not made by `claude-review-bot`.
- `chore/bump` PRs are not removed; they are flagged, and `feature_no_chore_bump` excludes them as a separate dataset.
- `kids_area_total` is computed as aggregation across all repos for the same sprint and dataset.

## Troubleshooting

- `401/403`: verify `BB_API_TOKEN` and `BB_USERNAME` (or `BB_EMAIL`), and confirm token permissions.
- Frequent `429`: increase `--throttle` (for example, `0.2` or `0.3`).
- `changes_requested_human` always `0`: this depends on your Bitbucket activity payload; the script calculates it on a best-effort basis.
