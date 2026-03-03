# Bitbucket PR Metrics (Kids Area)

Скрипт собирает метрики Pull Request из Bitbucket Cloud по репозиториям проекта и считает агрегаты по спринтам `Thu 07:30 UTC`.

## Requirements

- Python `3.12.x`
- Bitbucket App Password с правами:
  - `Repositories: Read`
  - `Pull requests: Read`

Проверка версии:

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
export BB_USERNAME="<your-bitbucket-username>"
export BB_APP_PASSWORD="<your-app-password>"
```

## Run

Пример для вашего workspace/project:

```bash
python3.12 bb_pr_metrics.py \
  --workspace novakidschool \
  --project-uuid "{6cadc6e0-6e64-46fa-b8c1-aa6468d65783}" \
  --sprints 8 \
  --out ./out
```

Полезные флаги:

- `--no-diffstat` — быстрее, пропускает diffstat.
- `--no-include-total` — не добавлять `kids_area_total` в CSV.
- `--no-report-md` — не генерировать markdown отчёт.

## Outputs

После запуска в `./out`:

1. `pr_facts.csv` — сырые факты по каждому PR.
2. `sprint_repo_metrics.csv` — метрики `repo × sprint × dataset`.
3. `sprint_report.md` — человекочитаемый отчёт для Confluence/Slack.

Dataset values:

- `feature_all`
- `feature_no_chore_bump`
- `releases`
- `hotfix_strict`
- `hotfix_candidates`
- `other`

## Metric rules

- Спринт: `Thu 07:30 UTC`, длина 14 дней.
- `first response`: любая активность не от автора PR и не от `claude-review-bot`.
- `chore/bump` PR не исключаются, а помечаются; отдельный dataset `feature_no_chore_bump` их исключает.
- `kids_area_total` строится как агрегация по всем repo в том же спринте и dataset.

## Troubleshooting

- `401/403`: проверьте `BB_USERNAME`/`BB_APP_PASSWORD` и права App Password.
- Частые `429`: увеличьте `--throttle` (например, `0.2` или `0.3`).
- `changes_requested_human` всегда `0`: это зависит от payload вашего Bitbucket activity, скрипт считает best-effort.
