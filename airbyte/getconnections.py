"""downloads workspace information from the airbyte database"""
import os
import sys
import logging
import argparse
import json
import psycopg2
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--workspace-id", required=True)
parser.add_argument("--outfile")
args = parser.parse_args()


def get_secret(secret_id: str):
    """retrieves a secret from GSM"""
    project_id = os.getenv("SECRET_STORE_GCP_PROJECT_ID")
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}"
    try:
        response = client.access_secret_version(
            request={"name": secret_name + "/versions/latest"}
        )
        return response.payload.data.decode("utf-8")
    except NotFound:
        return None


def get_actor_docker_image_tag(cursor, actor_definition_id: str):
    """
    returns the docker image tag for the actor definition
    the actor definition id will differ across airbyte installations
    for a custom connector
    """
    columns = ("docker_repository", "docker_image_tag")
    cursor.execute(
        f"""SELECT {",".join(columns)} FROM actor_definition_version
        WHERE actor_definition_id = '{actor_definition_id}'
        """
    )
    results = cursor.fetchall()
    docker_image = dict(zip(columns, results[0]))
    return f"{docker_image['docker_repository']}:{docker_image['docker_image_tag']}"


def get_actor(cursor, actor_type: str, actor_id: str):
    """get a single source or definition"""
    columns = (
        "id",
        "workspace_id",
        "actor_definition_id",
        "name",
        "configuration",
        "actor_type",
    )
    cursor.execute(
        f"""
          SELECT {",".join(columns)} FROM actor
          WHERE actor_type = '{actor_type}'
          AND id = '{actor_id}'
        """
    )
    results = cursor.fetchall()

    result = results[0]
    actor = dict(zip(columns, result))

    actor["docker_image_tag"] = get_actor_docker_image_tag(
        cursor, actor["actor_definition_id"]
    )

    # actor[f"{actor_type}_definition_id"] = actor["actor_definition_id"]
    del actor["actor_definition_id"]
    del actor["actor_type"]

    if "password" in actor["configuration"] and isinstance(
        actor["configuration"]["password"], dict
    ):
        actor["configuration"]["password"] = get_secret(
            actor["configuration"]["password"]["_secret"]
        )

    return actor


def get_source(cursor, source_id: str):
    """fetch a single source from the airbyte actor table"""
    source = get_actor(cursor, "source", source_id)
    return source


def get_destination(cursor, destination_id: str):
    """fetch a single destination from the airbyte actor table"""
    destination = get_actor(cursor, "destination", destination_id)
    return destination


def get_connections(cursor, workspace_id: str):
    """
    read the airbyte connections table
    """
    columns = (
        "connection.id AS id",
        "source_id",
        "destination_id",
        "namespace_definition",
        "namespace_format",
        "prefix",
        "connection.name AS name",
        "catalog",
        # "field_selection_data",
    )
    query = f"""
      SELECT {",".join(columns)} FROM connection
      JOIN actor ON connection.source_id = actor.id
      WHERE status = 'active'
      AND actor.workspace_id = '{workspace_id}'
    """
    cursor.execute(query)
    results = cursor.fetchall()

    return_value = {
        "sources": [],
        "destinations": [],
        "connections": [],
    }

    for result in results:
        connection = dict(zip(columns, result))

        source = get_source(cursor, connection["source_id"])
        destination = get_destination(cursor, connection["destination_id"])

        if source["id"] not in [x["id"] for x in return_value["sources"]]:
            return_value["sources"].append(source)
        if destination["id"] not in [x["id"] for x in return_value["destinations"]]:
            return_value["destinations"].append(destination)

        connection["id"] = connection["connection.id AS id"]
        del connection["connection.id AS id"]
        connection["name"] = connection["connection.name AS name"]
        del connection["connection.name AS name"]

        return_value["connections"].append(connection)

    return return_value


# -- start
conn = psycopg2.connect(
    host=os.getenv("DBHOST"),
    port=os.getenv("DBPORT"),
    user=os.getenv("DBUSER"),
    password=os.getenv("DBPASSWORD"),
    database=os.getenv("DBNAME"),
)
thecursor = conn.cursor()
data = get_connections(thecursor, args.workspace_id)

if args.outfile:
    with open(args.outfile, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=2)
else:
    json.dump(data, sys.stdout, indent=2)

conn.close()
