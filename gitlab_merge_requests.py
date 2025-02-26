# Script should be used to fetch Merge Requests (Pull Requests)
# from company GitLab space.
# It requires:
# - An access token
# - Company GitLab domain name
# - GitLab user ID
# - GitLab project IDs from the MRs will be fetched

import csv
import json
import os
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


@dataclass(frozen=True)
class GitlabMergeRequest:
    web_url: str = field(default="")
    description: str = field(default="")
    title: str = field(default="")
    created_at: str = field(default="")


load_dotenv()

REQUIRED_ENV_VARS = [
    "GITLAB_COMPANY_DOMAIN",
    "GITLAB_PRIVATE_TOKEN",
    "GITLAB_USER_ID",
    "GITLAB_PROJECT_IDS",
]

for env_var_name in REQUIRED_ENV_VARS:
    env_var = os.getenv(env_var_name)
    if not env_var:
        raise ValueError(f"{env_var_name} seems to be empty, should be set in .env")

GITLAB_COMPANY_DOMAIN = os.getenv("GITLAB_COMPANY_DOMAIN")
GITLAB_PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")
GITLAB_USER_ID = int(os.getenv("GITLAB_USER_ID"))
GITLAB_PROJECT_IDS = os.getenv("GITLAB_PROJECT_IDS").split(",")

START_TIMESTAMP = ""  # format: 'YYYY-MM-DD HH:MM:SS'
if not START_TIMESTAMP:
    raise ValueError("START_TIMESTAMP seems to be empty, should be set in this file")
END_TIMESTAMP = ""  # format: 'YYYY-MM-DD HH:MM:SS'
if not END_TIMESTAMP:
    raise ValueError("END_TIMESTAMP seems to be empty, should be set in this file")
CSV_RESULTS_DIR = "results"
if not CSV_RESULTS_DIR:
    raise ValueError("CSV_RESULTS_DIR seems to be empty, should be set in this file")
CSV_RESULTS_FILENAME = "gitlab-merge-requests"  # format: <name> ; rest, including .csv suffix will be generated
if not CSV_RESULTS_FILENAME:
    raise ValueError(
        "CSV_RESULTS_FILENAME seems to be empty, should be set in this file"
    )

PARSED_START_TIMESTAMP = datetime.strptime(
    START_TIMESTAMP, "%Y-%m-%d %H:%M:%S"
).replace(tzinfo=timezone.utc)
PARSED_END_TIMESTAMP = datetime.strptime(END_TIMESTAMP, "%Y-%m-%d %H:%M:%S").replace(
    tzinfo=timezone.utc
)


def filter_merge_requests(mr):
    parsed_created_at = datetime.strptime(
        mr["created_at"][:-1], "%Y-%m-%dT%H:%M:%S.%f"
    ).replace(tzinfo=timezone.utc)
    return (
        True
        if mr["state"] != "closed"
        and parsed_created_at > PARSED_START_TIMESTAMP
        and parsed_created_at < PARSED_END_TIMESTAMP
        else False
    )


def create_results_file_path():
    curr_datetime = datetime.now()
    return f"{CSV_RESULTS_DIR}/{CSV_RESULTS_FILENAME}-{curr_datetime.year}-{curr_datetime.month}.csv"


headers = {"PRIVATE-TOKEN": GITLAB_PRIVATE_TOKEN}

merge_requests_list = []
for project_id in GITLAB_PROJECT_IDS:
    response = requests.get(
        f"https://git.{GITLAB_COMPANY_DOMAIN}/api/v4/projects/{project_id}/merge_requests?state=all&author_id={GITLAB_USER_ID}",
        headers=headers,
    )

    if response.status_code == 200:
        merge_requests_list = merge_requests_list + json.loads(response.text)


merge_requests_filtered = list(filter(filter_merge_requests, merge_requests_list))
gitlab_merge_requests = map(
    lambda mr: GitlabMergeRequest(
        web_url=mr["web_url"],
        description=mr["description"],
        title=mr["title"],
        created_at=mr["created_at"],
    ),
    merge_requests_filtered,
)

csv_file_path = create_results_file_path()
with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(
        file, fieldnames=[field.name for field in fields(GitlabMergeRequest)]
    )
    writer.writeheader()
    for mr in gitlab_merge_requests:
        writer.writerow(mr.__dict__)
