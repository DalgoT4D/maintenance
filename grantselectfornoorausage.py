"""grants SELECT privilege on all tables in the noora schema to the user noora_usage_user"""

import os
import psycopg2

from dotenv import load_dotenv

load_dotenv(".env.grantselectfornoorausage")

conn = psycopg2.connect(
    host=os.getenv("NOORA_DB_HOST"),
    port=os.getenv("NOORA_DB_PORT"),
    user=os.getenv("NOORA_DB_USER"),
    password=os.getenv("NOORA_DB_PASSWORD"),
    database=os.getenv("NOORA_DB_NAME"),
)

cursor = conn.cursor()
cursor.execute("GRANT SELECT ON noora_health.ab_role TO noora_usage_user")
cursor.execute("GRANT SELECT ON noora_health.ab_user TO noora_usage_user")
cursor.execute("GRANT SELECT ON noora_health.ab_user_role TO noora_usage_user")
cursor.execute("GRANT SELECT ON noora_health.dashboard_roles TO noora_usage_user")
cursor.execute("GRANT SELECT ON noora_health.dashboards TO noora_usage_user")
cursor.execute("GRANT SELECT ON noora_health.slices TO noora_usage_user")
cursor.close()
