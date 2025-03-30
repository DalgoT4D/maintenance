"""Script to backup all PostgreSQL databases and upload them to S3"""

import os
import datetime
import subprocess
import tempfile
import argparse
import pytz
import boto3
import psycopg2
from dotenv import load_dotenv

parser = argparse.ArgumentParser(description="Backup all PostgreSQL databases")
parser.add_argument(
    "--dry-run", action="store_true", help="Run the script in dry-run mode"
)
args = parser.parse_args()
logprefix = "[DRY RUN] " if args.dry_run else ""

# continue if .env file is not found, e.g. when we run from prefect
load_dotenv(".env.dailybackup")

# Configuration
PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_BACKUP_PATH = os.getenv("S3_BACKUP_PATH")
RETENTION_DAYS = os.getenv("RETENTION_DAYS")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
PGDUMP_BINARY = os.getenv("PGDUMP_BINARY", "/usr/lib/postgresql/15/bin/pg_dump")
ROOT_CERT_LOCATION = os.getenv(
    "ROOT_CERT_LOCATION", "/home/ddp/rds-combined-ca-bundle.pem"
)

IST = pytz.timezone("Asia/Kolkata")

# Establish a connection to PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    sslmode="allow",
    sslrootcert=ROOT_CERT_LOCATION,
)
cursor = conn.cursor()

# Fetch all databases except template and system databases
cursor.execute(
    """
    SELECT datname FROM pg_database
    WHERE datistemplate = false AND datname != 'postgres' AND datname != 'rdsadmin';
"""
)
databases = [row[0] for row in cursor.fetchall()]

# Close the connection
cursor.close()
conn.close()

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

# Get today's date
timestamp = datetime.datetime.now(IST).strftime("%Y-%m-%d")

# Create a temporary directory
with tempfile.TemporaryDirectory() as tmpdirname:
    print(f"Created temporary directory: {tmpdirname}")

    # Backup each database
    for db in databases:
        backup_file = f"{db}_{timestamp}.sql.gz"
        backup_file_dir = os.path.join(tmpdirname, backup_file)

        dump_cmd = f'{PGDUMP_BINARY} "host={PG_HOST} user={PG_USER} dbname={db} sslrootcert={ROOT_CERT_LOCATION} sslmode=allow" | gzip > {backup_file_dir}'
        s3_key = f"{S3_BACKUP_PATH}/{backup_file}"
        print(f"{logprefix} CMD: PGPASSWORD=<*********> {dump_cmd}")
        dump_cmd = f"PGPASSWORD={PG_PASSWORD} " + dump_cmd
        print(f"{logprefix} KEY: {s3_key}")

        if args.dry_run:
            continue

        # Run pg_dump
        subprocess.run(dump_cmd, shell=True, check=True)
        # Upload to S3
        s3_client.upload_file(backup_file_dir, S3_BUCKET, s3_key)

print(f"{logprefix} Backups completed. Now cleaning up old backups...")

# Delete old backups from S3
expiration_date = (
    datetime.datetime.utcnow() - datetime.timedelta(days=int(RETENTION_DAYS))
).strftime("%Y-%m-%d")

response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_BACKUP_PATH)
if "Contents" in response:
    for obj in response["Contents"]:
        obj_date = obj["Key"].split("_")[-1].replace(".sql.gz", "")
        if obj_date < expiration_date:
            print(f"{logprefix} Deleting {obj['Key']} from s3 bucket {S3_BUCKET}")
            if args.dry_run:
                continue
            s3_client.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
            print(f"Deleted {obj['Key']} from S3")

print("Backup process completed.")
