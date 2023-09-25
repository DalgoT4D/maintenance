"""monitor databases having a high number of idle connections"""
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

load_dotenv("pgmonitor.env")

rdsurl = os.getenv("RDS_URL")

conn = psycopg2.connect(
    host=rdsurl, user=os.getenv("RDS_USER"), password=os.getenv("RDS_PASSWORD")
)

cursor = conn.cursor()

cursor.execute(
    "SELECT datname, count(1) FROM pg_stat_activity WHERE datname IS NOT NULL GROUP BY datname"
)

results = cursor.fetchall()

dtnow = datetime.now()
date = dtnow.strftime("%Y-%m-%d")
time = dtnow.strftime("%H:%M")
logfilename = f'{os.getenv("RESULTSFILEPREFIX")}.{date}.txt'

with open(logfilename, "a", encoding="utf-8") as logfile:
    for datname, nconnections in results:
        if nconnections > 4:
            logfile.write(f"{time} {datname} {nconnections}\n")
