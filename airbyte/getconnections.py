"""downloads workspace information from the airbyte database"""
import os
import argparse
import json
import psycopg2
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound

from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("--for-migration", action="store_true")
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


def get_sources(cursor):
    """fetch all sources from the airbyte actor table"""
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
      SELECT {",".join(columns)} FROM actor WHERE actor_type = 'source'
    """
    )
    results = cursor.fetchall()

    for result in results:
        source = dict(zip(columns, result))
        source["source_definition_id"] = source["actor_definition_id"]
        del source["actor_definition_id"]
        del source["actor_type"]
        if "password" in source["configuration"] and isinstance(
            source["configuration"]["password"], dict
        ):
            source["configuration"]["password"] = get_secret(
                source["configuration"]["password"]["_secret"]
            )
        print(json.dumps(source, indent=2))


def get_destinations(cursor):
    """fetch all destinations from the airbyte actor table"""
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
      SELECT {",".join(columns)} FROM actor WHERE actor_type = 'destination'
    """
    )
    results = cursor.fetchall()

    for result in results:
        destination = dict(zip(columns, result))

        destination["docker_image_tag"] = get_actor_docker_image_tag(
            cursor, destination["actor_definition_id"]
        )

        if args.for_migration:
            del destination["actor_definition_id"]
            del destination["actor_type"]
            del destination["id"]
            del destination["workspace_id"]
        else:
            destination["destination_definition_id"] = destination[
                "actor_definition_id"
            ]
            del destination["actor_definition_id"]
            del destination["actor_type"]

        if "password" in destination["configuration"] and isinstance(
            destination["configuration"]["password"], dict
        ):
            destination["configuration"]["password"] = get_secret(
                destination["configuration"]["password"]["_secret"]
            )
        print(json.dumps(destination, indent=2))


# -- start
conn = psycopg2.connect(
    host=os.getenv("DBHOST"),
    port=os.getenv("DBPORT"),
    user=os.getenv("DBUSER"),
    password=os.getenv("DBPASSWORD"),
    database=os.getenv("DBNAME"),
)
thecursor = conn.cursor()
get_sources(thecursor)
get_destinations(thecursor)
