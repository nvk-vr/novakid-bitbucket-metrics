#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

BOT_USERS = {"claude-review-bot"}  # exclude from first-response + review stats

# Branching rules
DEPLOY_DEST_EXCLUDE_PREFIXES = ("release/", "chore/")
DEPLOY_DEST_EXCLUDE_EXACT = {"master", "release", "chore"}
RELEASE_SOURCE_PREFIXES = ("release", "release/")

# Feature target per repo
FEATURE_TARGET_DEFAULT = "dev"
FEATURE_TARGET_OVERRIDES = {
    "novakid-skill-exerciser-web-core": "main",
}


def is_chore_or_bump(title: str) -> bool:
    t = (title or "").strip().lower()
    return t.startswith("chore") or ("bump" in t)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


@dataclass(frozen=True)
class Sprint:
    idx: int
    start: datetime
    end: datetime


def wet_boundary_before(now_utc: datetime) -> datetime:
    """
    Sprint boundaries: Thu 07:30 WET.
    WET = UTC+0, so we can work in UTC directly.
    """
    now_utc = now_utc.astimezone(timezone.utc)

    # weekday(): Mon=0 .. Sun=6, Thu=3
    target_weekday = 3
    boundary_time = timedelta(hours=7, minutes=30)

    today = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
    candidate = today + boundary_time

    days_back = (candidate.weekday() - target_weekday) % 7
    candidate = candidate - timedelta(days=days_back)

    if candidate > now_utc:
        candidate = candidate - timedelta(days=7)

    return candidate


def build_sprints(now_utc: datetime, count: int) -> List[Sprint]:
    end = wet_boundary_before(now_utc)
    sprints: List[Sprint] = []
    for i in range(count):
        start = end - timedelta(days=14)
        sprints.append(Sprint(idx=count - i, start=start, end=end))
        end = start
    return list(reversed(sprints))


class BBClient:
    def __init__(self, workspace: str, auth_user: str, auth_secret: str, throttle_s: float = 0.1):
        self.workspace = workspace
        self.session = requests.Session()
        self.session.auth = (auth_user, auth_secret)
        self.session.headers.update({"Accept": "application/json"})
        self.throttle_s = throttle_s

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = self.session.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            time.sleep(2.0)
            resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        if self.throttle_s:
            time.sleep(self.throttle_s)
        return resp.json()

    def iter_paginated(self, url: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
        next_url = url
        next_params = dict(params or {})
        while next_url:
            data = self._get(next_url, next_params)
            for v in data.get("values", []):
                yield v
            next_url = data.get("next")
            next_params = None

    def list_repos_in_project(self, project_uuid: str) -> List[Dict[str, Any]]:
        url = f"https://api.bitbucket.org/2.0/repositories/{self.workspace}"
        q = f'project.uuid="{project_uuid}"'
        repos = list(self.iter_paginated(url, params={"q": q, "pagelen": 100}))
        return repos

    def list_pullrequests(self, repo_slug: str, since_utc: datetime) -> List[Dict[str, Any]]:
        url = f"https://api.bitbucket.org/2.0/repositories/{self.workspace}/{repo_slug}/pullrequests"
        q = f'(created_on >= "{iso(since_utc)}" OR updated_on >= "{iso(since_utc)}")'
        prs = list(self.iter_paginated(url, params={"state": "ALL", "q": q, "pagelen": 50, "sort": "-updated_on"}))
        return prs

    def iter_pr_activity(self, repo_slug: str, pr_id: int) -> Iterable[Dict[str, Any]]:
        url = f"https://api.bitbucket.org/2.0/repositories/{self.workspace}/{repo_slug}/pullrequests/{pr_id}/activity"
        yield from self.iter_paginated(url, params={"pagelen": 50})

    def get_pr_diffstat(self, repo_slug: str, pr_id: int) -> Tuple[int, int]:
        url = f"https://api.bitbucket.org/2.0/repositories/{self.workspace}/{repo_slug}/pullrequests/{pr_id}/diffstat"
        files = 0
        lines = 0
        for item in self.iter_paginated(url, params={"pagelen": 100}):
            files += 1
            lines += int(item.get("lines_added", 0)) + int(item.get("lines_removed", 0))
        return lines, files


def branch_name(obj: Optional[Dict[str, Any]]) -> str:
    if not obj:
        return ""
    return (((obj.get("branch") or {}).get("name")) or "").strip()


def user_nick(user_obj: Optional[Dict[str, Any]]) -> str:
    if not user_obj:
        return ""
    return (
        user_obj.get("nickname")
        or user_obj.get("username")
        or user_obj.get("display_name")
        or ""
    ).strip()


def classify_pr(repo_slug: str, src: str, dst: str) -> str:
    if dst == "release" or dst.startswith("release/") or src == "release" or src.startswith("release/"):
        return "release"

    if dst == "master" and src.startswith("hotfix/") and not (src == "release" or src.startswith("release/")):
        return "hotfix_strict"

    if dst == "master":
        return "master_other"

    feature_target = FEATURE_TARGET_OVERRIDES.get(repo_slug, FEATURE_TARGET_DEFAULT)
    if dst == feature_target:
        return "feature"

    return "other"


def is_deploy_destination(dst: str) -> bool:
    if dst in DEPLOY_DEST_EXCLUDE_EXACT:
        return True
    return any(dst.startswith(p) for p in DEPLOY_DEST_EXCLUDE_PREFIXES)


def bucket_size(lines_changed: int) -> str:
    if lines_changed <= 50:
        return "S"
    if lines_changed <= 200:
        return "M"
    if lines_changed <= 800:
        return "L"
    return "XL"


@dataclass
class PRFacts:
    repo: str
    pr_id: int
    title: str
    state: str
    author: str
    source: str
    destination: str
    created_at: datetime
    updated_at: datetime

    pr_type: str
    is_chore_or_bump: bool

    first_response_at: Optional[datetime]
    first_response_actor: Optional[str]

    merged_at: Optional[datetime]
    declined_at: Optional[datetime]

    comments_human: int
    approvals_human: int
    changes_requested_human: int

    lines_changed: int
    files_changed: int
    size_bucket: str


def extract_facts(
    bb: BBClient,
    repo_slug: str,
    pr: Dict[str, Any],
    compute_diffstat: bool = True,
) -> PRFacts:
    pr_id = int(pr["id"])
    title = pr.get("title") or ""
    state = pr.get("state") or ""
    author = user_nick((pr.get("author") or {}).get("user") or pr.get("author"))
    src = branch_name((pr.get("source") or {}).get("branch"))
    dst = branch_name((pr.get("destination") or {}).get("branch"))

    created_at = parse_iso(pr["created_on"])
    updated_at = parse_iso(pr["updated_on"])

    pr_type = classify_pr(repo_slug, src, dst)
    chore_bump = is_chore_or_bump(title)

    first_resp_at: Optional[datetime] = None
    first_resp_actor: Optional[str] = None
    merged_at: Optional[datetime] = None
    declined_at: Optional[datetime] = None
    comments = 0
    approvals = 0
    changes_req = 0

    author_key = author.lower()

    for act in bb.iter_pr_activity(repo_slug, pr_id):
        actor = ""
        ts: Optional[datetime] = None

        if "comment" in act:
            c = act["comment"]
            actor = user_nick((c.get("user") or {}))
            ts = parse_iso(c.get("created_on")) if c.get("created_on") else None
            if actor and actor.lower() != author_key and actor not in BOT_USERS:
                comments += 1

        elif "approval" in act:
            a = act["approval"]
            actor = user_nick((a.get("user") or {}))
            ts = parse_iso(a.get("date")) if a.get("date") else None
            if actor and actor.lower() != author_key and actor not in BOT_USERS:
                approvals += 1

        elif "changes_requested" in act:
            cr = act["changes_requested"]
            actor = user_nick((cr.get("user") or {}))
            ts = parse_iso(cr.get("date")) if cr.get("date") else None
            if actor and actor.lower() != author_key and actor not in BOT_USERS:
                changes_req += 1

        elif "update" in act:
            u = act["update"]
            actor = user_nick((u.get("author") or u.get("user") or {}))
            ts = parse_iso(u.get("date")) if u.get("date") else None

        elif "pullrequest" in act:
            pr_ev = act["pullrequest"]
            ts_field = pr_ev.get("date") or pr_ev.get("created_on") or pr_ev.get("updated_on")
            ts = parse_iso(ts_field) if ts_field else None
            actor = user_nick((pr_ev.get("user") or pr_ev.get("actor") or {}))
            new_state = (pr_ev.get("state") or "").upper()
            if new_state == "MERGED" and merged_at is None and ts is not None:
                merged_at = ts
            if new_state == "DECLINED" and declined_at is None and ts is not None:
                declined_at = ts

        if ts is not None and actor:
            if actor.lower() != author_key and actor not in BOT_USERS:
                if first_resp_at is None or ts < first_resp_at:
                    first_resp_at = ts
                    first_resp_actor = actor

    if state.upper() == "MERGED" and merged_at is None:
        merged_at = updated_at
    if state.upper() == "DECLINED" and declined_at is None:
        declined_at = updated_at

    lines_changed, files_changed = (0, 0)
    if compute_diffstat:
        try:
            lines_changed, files_changed = bb.get_pr_diffstat(repo_slug, pr_id)
        except Exception:
            lines_changed, files_changed = (0, 0)

    size_b = bucket_size(lines_changed)

    return PRFacts(
        repo=repo_slug,
        pr_id=pr_id,
        title=title,
        state=state.upper(),
        author=author,
        source=src,
        destination=dst,
        created_at=created_at,
        updated_at=updated_at,
        pr_type=pr_type,
        is_chore_or_bump=chore_bump,
        first_response_at=first_resp_at,
        first_response_actor=first_resp_actor,
        merged_at=merged_at,
        declined_at=declined_at,
        comments_human=comments,
        approvals_human=approvals,
        changes_requested_human=changes_req,
        lines_changed=lines_changed,
        files_changed=files_changed,
        size_bucket=size_b,
    )


def sprint_for(ts: Optional[datetime], sprints: List[Sprint]) -> Optional[Sprint]:
    if ts is None:
        return None
    for sp in sprints:
        if sp.start <= ts < sp.end:
            return sp
    return None


def percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def to_hours(td: timedelta) -> float:
    return td.total_seconds() / 3600.0


def agg_stats(durations_h: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    vals = sorted([x for x in durations_h if x is not None])
    if not vals:
        return None, None, None
    med = percentile(vals, 50)
    p75 = percentile(vals, 75)
    p90 = percentile(vals, 90)
    return med, p75, p90


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def dataset_of(x: PRFacts) -> str:
    if x.pr_type == "feature":
        return "feature_all"
    if x.pr_type == "hotfix_strict":
        return "hotfix_strict"
    if x.pr_type == "release":
        return "releases"
    if x.pr_type == "master_other":
        return "hotfix_candidates"
    return "other"


def closed_ts(x: PRFacts) -> Optional[datetime]:
    if x.state == "MERGED":
        return x.merged_at
    if x.state == "DECLINED":
        return x.declined_at
    return None


def row_metrics_for_group(xs: List[PRFacts], sp: Sprint) -> Dict[str, Any]:
    opened = [x for x in xs if sprint_for(x.created_at, [sp]) is not None]
    closed = [x for x in xs if sprint_for(closed_ts(x), [sp]) is not None]

    merged = [x for x in closed if x.state == "MERGED"]
    declined = [x for x in closed if x.state == "DECLINED"]

    tfr_list: List[float] = []
    no_resp = 0
    for x in opened:
        if x.first_response_at:
            tfr_list.append(to_hours(x.first_response_at - x.created_at))
        else:
            no_resp += 1

    pct_no_resp = (100.0 * no_resp / len(opened)) if opened else None
    tfr_med, tfr_p75, tfr_p90 = agg_stats(tfr_list)

    ttm_list: List[float] = []
    for x in merged:
        if x.merged_at:
            ttm_list.append(to_hours(x.merged_at - x.created_at))
    ttm_med, ttm_p75, ttm_p90 = agg_stats(ttm_list)

    comments_vals = sorted([float(x.comments_human) for x in opened])
    comments_med = percentile(comments_vals, 50) if comments_vals else None
    comments_p90 = percentile(comments_vals, 90) if comments_vals else None

    pct_rc = None
    if opened:
        hits = sum(1 for x in opened if x.changes_requested_human > 0)
        pct_rc = 100.0 * hits / len(opened)

    size_s = size_m = size_l = size_xl = 0
    for x in opened:
        if x.size_bucket == "S":
            size_s += 1
        elif x.size_bucket == "M":
            size_m += 1
        elif x.size_bucket == "L":
            size_l += 1
        elif x.size_bucket == "XL":
            size_xl += 1

    return {
        "opened_count": len(opened),
        "merged_count": len(merged),
        "declined_count": len(declined),
        "pct_no_first_response": pct_no_resp,
        "t_first_response_med_h": tfr_med,
        "t_first_response_p75_h": tfr_p75,
        "t_first_response_p90_h": tfr_p90,
        "t_merge_med_h": ttm_med,
        "t_merge_p75_h": ttm_p75,
        "t_merge_p90_h": ttm_p90,
        "comments_med": comments_med,
        "comments_p90": comments_p90,
        "pct_requested_changes": pct_rc,
        "size_S": size_s,
        "size_M": size_m,
        "size_L": size_l,
        "size_XL": size_xl,
    }


def maybe_round(x: Optional[float]) -> str:
    if x is None:
        return ""
    return str(round(x, 2))


def build_markdown_report(
    rows_dict: List[Dict[str, Any]],
    sprints: List[Sprint],
    workspace: str,
    project_uuid: str,
    out_path: str,
) -> None:
    by_sprint: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows_dict:
        by_sprint[int(row["sprint_idx"])].append(row)

    lines: List[str] = []
    lines.append("# Bitbucket PR Metrics Report")
    lines.append("")
    lines.append(f"- Workspace: `{workspace}`")
    lines.append(f"- Project UUID: `{project_uuid}`")
    lines.append(f"- Generated at (UTC): `{iso(datetime.now(timezone.utc))}`")
    lines.append("")

    sprint_map = {sp.idx: sp for sp in sprints}
    for sprint_idx in sorted(by_sprint.keys(), reverse=True):
        sp = sprint_map[sprint_idx]
        rows = by_sprint[sprint_idx]
        lines.append(f"## Sprint {sprint_idx}: {iso(sp.start)} .. {iso(sp.end)}")
        lines.append("")
        lines.append("| Repo | Dataset | Opened | Merged | Declined | No First Resp % | TFR Med (h) | TFR P90 (h) | Merge Med (h) | Merge P90 (h) | Req Changes % |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

        # kids_area_total first, then rest by repo/dataset
        rows = sorted(
            rows,
            key=lambda r: (
                0 if r["repo"] == "kids_area_total" else 1,
                r["repo"],
                r["dataset"],
            ),
        )
        for r in rows:
            lines.append(
                "| {repo} | {dataset} | {opened} | {merged} | {declined} | {p_no} | {tfr_m} | {tfr_p90} | {tm_m} | {tm_p90} | {p_rc} |".format(
                    repo=r["repo"],
                    dataset=r["dataset"],
                    opened=r["opened_count"],
                    merged=r["merged_count"],
                    declined=r["declined_count"],
                    p_no=maybe_round(r["pct_no_first_response"]),
                    tfr_m=maybe_round(r["t_first_response_med_h"]),
                    tfr_p90=maybe_round(r["t_first_response_p90_h"]),
                    tm_m=maybe_round(r["t_merge_med_h"]),
                    tm_p90=maybe_round(r["t_merge_p90_h"]),
                    p_rc=maybe_round(r["pct_requested_changes"]),
                )
            )
        lines.append("")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", required=True)
    ap.add_argument("--project-uuid", required=True, help='e.g. "{uuid}" or "uuid"')
    ap.add_argument("--sprints", type=int, default=8)
    ap.add_argument("--out", default="./out")
    ap.add_argument("--throttle", type=float, default=0.1)
    ap.add_argument("--no-diffstat", action="store_true", help="skip diffstat (faster)")
    ap.add_argument(
        "--include-total",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="include kids_area_total rows in sprint_repo_metrics.csv",
    )
    ap.add_argument(
        "--report-md",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="generate out/sprint_report.md",
    )
    args = ap.parse_args()

    # Bitbucket API token auth:
    #   BB_USERNAME + BB_API_TOKEN (preferred)
    #   BB_EMAIL + BB_API_TOKEN (fallback)
    bb_username = os.environ.get("BB_USERNAME", "").strip()
    bb_email = os.environ.get("BB_EMAIL", "").strip()
    bb_api_token = os.environ.get("BB_API_TOKEN", "").strip()

    auth_user = ""
    auth_secret = ""

    if bb_api_token and (bb_username or bb_email):
        auth_user = bb_username or bb_email
        auth_secret = bb_api_token
    else:
        print(
            "ERROR: Set BB_API_TOKEN and one of BB_USERNAME/BB_EMAIL env vars.",
            file=sys.stderr,
        )
        return 2

    project_uuid = args.project_uuid.strip()
    if project_uuid.startswith("{") and project_uuid.endswith("}"):
        project_uuid = project_uuid[1:-1]

    bb = BBClient(args.workspace, auth_user, auth_secret, throttle_s=args.throttle)

    now = datetime.now(timezone.utc)
    sprints = build_sprints(now, args.sprints)
    since = sprints[0].start

    ensure_dir(args.out)

    print(f"Fetching repos in project {project_uuid} ...")
    repos = bb.list_repos_in_project(project_uuid)
    repo_slugs = sorted({r["slug"] for r in repos})

    print(f"Found {len(repo_slugs)} repos. Sprint range: {since} .. {sprints[-1].end}")

    all_pr_facts: List[PRFacts] = []

    for repo_slug in repo_slugs:
        print(f"\n== Repo: {repo_slug}")
        prs = bb.list_pullrequests(repo_slug, since_utc=since)
        print(f"PR candidates since {since.date()}: {len(prs)}")

        for pr in prs:
            try:
                facts = extract_facts(bb, repo_slug, pr, compute_diffstat=not args.no_diffstat)
                all_pr_facts.append(facts)
            except Exception as e:
                print(f"  ! Failed PR #{pr.get('id')} in {repo_slug}: {e}", file=sys.stderr)

    pr_csv = os.path.join(args.out, "pr_facts.csv")
    with open(pr_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "repo",
            "pr_id",
            "state",
            "pr_type",
            "is_chore_or_bump",
            "title",
            "author",
            "source",
            "destination",
            "created_at",
            "updated_at",
            "merged_at",
            "declined_at",
            "first_response_at",
            "first_response_actor",
            "time_to_first_response_h",
            "time_to_merge_h",
            "comments_human",
            "approvals_human",
            "changes_requested_human",
            "lines_changed",
            "files_changed",
            "size_bucket",
        ])
        for x in all_pr_facts:
            tfr = to_hours(x.first_response_at - x.created_at) if x.first_response_at else ""
            ttm = to_hours(x.merged_at - x.created_at) if x.merged_at and x.state == "MERGED" else ""
            w.writerow([
                x.repo,
                x.pr_id,
                x.state,
                x.pr_type,
                int(x.is_chore_or_bump),
                x.title,
                x.author,
                x.source,
                x.destination,
                iso(x.created_at),
                iso(x.updated_at),
                iso(x.merged_at) if x.merged_at else "",
                iso(x.declined_at) if x.declined_at else "",
                iso(x.first_response_at) if x.first_response_at else "",
                x.first_response_actor or "",
                tfr,
                ttm,
                x.comments_human,
                x.approvals_human,
                x.changes_requested_human,
                x.lines_changed,
                x.files_changed,
                x.size_bucket,
            ])

    header = [
        "sprint_idx",
        "sprint_start",
        "sprint_end",
        "repo",
        "dataset",
        "opened_count",
        "merged_count",
        "declined_count",
        "pct_no_first_response",
        "t_first_response_med_h",
        "t_first_response_p75_h",
        "t_first_response_p90_h",
        "t_merge_med_h",
        "t_merge_p75_h",
        "t_merge_p90_h",
        "comments_med",
        "comments_p90",
        "pct_requested_changes",
        "size_S",
        "size_M",
        "size_L",
        "size_XL",
    ]

    group_repo: Dict[Tuple[int, str, str], List[PRFacts]] = defaultdict(list)
    group_total: Dict[Tuple[int, str], List[PRFacts]] = defaultdict(list)

    for x in all_pr_facts:
        ds = dataset_of(x)
        ds_variants = [ds]
        if ds == "feature_all" and not x.is_chore_or_bump:
            ds_variants.append("feature_no_chore_bump")

        sp_created = sprint_for(x.created_at, sprints)
        sp_closed = sprint_for(closed_ts(x), sprints)

        for sp in {sp_created, sp_closed}:
            if sp is None:
                continue
            for dsv in ds_variants:
                group_repo[(sp.idx, x.repo, dsv)].append(x)
                group_total[(sp.idx, dsv)].append(x)

    rows_dict: List[Dict[str, Any]] = []

    for (sprint_idx, repo, ds), xs in sorted(group_repo.items(), key=lambda k: (k[0][0], k[0][1], k[0][2])):
        sp = next((s for s in sprints if s.idx == sprint_idx), None)
        if sp is None:
            continue
        metrics = row_metrics_for_group(xs, sp)
        row = {
            "sprint_idx": sprint_idx,
            "sprint_start": iso(sp.start),
            "sprint_end": iso(sp.end),
            "repo": repo,
            "dataset": ds,
        }
        row.update(metrics)
        rows_dict.append(row)

    if args.include_total:
        for (sprint_idx, ds), xs in sorted(group_total.items(), key=lambda k: (k[0][0], k[0][1])):
            sp = next((s for s in sprints if s.idx == sprint_idx), None)
            if sp is None:
                continue
            metrics = row_metrics_for_group(xs, sp)
            row = {
                "sprint_idx": sprint_idx,
                "sprint_start": iso(sp.start),
                "sprint_end": iso(sp.end),
                "repo": "kids_area_total",
                "dataset": ds,
            }
            row.update(metrics)
            rows_dict.append(row)

    rows_dict.sort(key=lambda r: (r["sprint_idx"], r["repo"], r["dataset"]))

    out_csv = os.path.join(args.out, "sprint_repo_metrics.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows_dict:
            w.writerow([
                r["sprint_idx"],
                r["sprint_start"],
                r["sprint_end"],
                r["repo"],
                r["dataset"],
                r["opened_count"],
                r["merged_count"],
                r["declined_count"],
                maybe_round(r["pct_no_first_response"]),
                maybe_round(r["t_first_response_med_h"]),
                maybe_round(r["t_first_response_p75_h"]),
                maybe_round(r["t_first_response_p90_h"]),
                maybe_round(r["t_merge_med_h"]),
                maybe_round(r["t_merge_p75_h"]),
                maybe_round(r["t_merge_p90_h"]),
                maybe_round(r["comments_med"]),
                maybe_round(r["comments_p90"]),
                maybe_round(r["pct_requested_changes"]),
                r["size_S"],
                r["size_M"],
                r["size_L"],
                r["size_XL"],
            ])

    report_path = os.path.join(args.out, "sprint_report.md")
    if args.report_md:
        build_markdown_report(rows_dict, sprints, args.workspace, project_uuid, report_path)

    print("\nDone.")
    print(f"- PR facts: {pr_csv}")
    print(f"- Metrics:  {out_csv}")
    if args.report_md:
        print(f"- Report:   {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
