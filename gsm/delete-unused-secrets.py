"""identifies and deletes unused secrets in google's secret manager"""
# pylint:disable=invalid-name
import os

import json
import argparse
import logging
from google.oauth2 import service_account
from google.cloud import secretmanager
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, filename=os.getenv("LOGFILENAME"))
logger = logging.getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("--keep")
args = parser.parse_args()

parent = f"projects/{os.getenv('PROJECT_ID')}"

workspaces_to_keep = []
if args.keep:
    with open(args.keep, "r", encoding="utf8") as keepfile:
        keep = json.load(keepfile)
        workspaces_to_keep = keep["workspaces"]

credentials = service_account.Credentials.from_service_account_file(
    os.getenv("SERVICE_ACCOUNT_KEY_FILE")
)
client = secretmanager.SecretManagerServiceClient(credentials=credentials)

response = client.list_secrets(parent=parent)
for secretholder in response:
    # secretholder.name = projects/*/secrets/*
    # client.delete_secret(name=secretholder.name)
    secret_identifier = secretholder.name.split("/")[3]
    # pylint:disable=line-too-long
    # secret_identifier = airbyte_workspace_1519df85-0917-4da8-9852-7d160e96af31_secret_2e5d7db9-280b-4f07-918b-f19a898d4d0b_v1
    secret_identifier_fields = secret_identifier.split("_")
    if (
        len(secret_identifier_fields) == 6
        and secret_identifier_fields[0] == "airbyte"
        and secret_identifier_fields[1] == "workspace"
        and secret_identifier_fields[3] == "secret"
    ):
        workspace_id = secret_identifier_fields[2]
        workspace_secret_id = secret_identifier_fields[4]
    else:
        logger.warning("could-not-parse-secret-identifier: %s", secret_identifier)
        # pylint:disable=invalid-name
        workspace_id = "<unknown>"
        workspace_secret_id = "<unknown>"

    secret = client.access_secret_version(name=f"{secretholder.name}/versions/latest")
    logger.info("==")
    logger.info(
        "workspace_id=%s workspace_secret_id=%s", workspace_id, workspace_secret_id
    )
    logger.info(secret.payload.data)
    if workspace_id not in workspaces_to_keep:
        logger.info("DELETE %s", workspace_id)
