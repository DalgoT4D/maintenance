#!/usr/bin/env python3

import os
import argparse
import logging
from datetime import datetime, timedelta
import psycopg2
from yaml import safe_load
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(file=os.getenv("LOGFILE"), level=logging.INFO)
logger = logging.getLogger()

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--date")
args = parser.parse_args()

report_date = (
    datetime.strptime(args.date, "%Y-%m-%d")
    if args.date
    else datetime.today() - timedelta(days=1)
)


def get_conn(warehouse: dict):
    """opens a connection to the postgres database"""
    return psycopg2.connect(
        host=warehouse["HOST"],
        user=warehouse["USER"],
        password=warehouse["PASS"],
        database=warehouse["NAME"],
    )


def get_tables_in_schema(conn, warehouse: dict) -> list:
    """returns list of table names in the given schema"""

    cursor = conn.cursor()
    query = f"""SELECT table_name 
                FROM information_schema.tables
                WHERE table_schema = '{warehouse['SCHEMA']}'
                    AND table_catalog = '{warehouse["NAME"]}'
                """
    logger.info(query)
    cursor.execute(query)

    results = cursor.fetchall()
    tables = [x[0] for x in results]
    logger.info(tables)
    return tables


def get_sync_counts_from_table(
    conn, warehouse: dict, tblname: str, date: datetime
) -> dict:
    """reads the staging table to get records synced by date"""

    cursor = conn.cursor()
    datestr = date.strftime("%Y-%m-%d")
    query = f"""SELECT count(1)
                FROM {warehouse['SCHEMA']}.{tblname} 
                WHERE DATE(_airbyte_emitted_at) = '{datestr}'
                """
    logger.info(query)
    cursor.execute(query)

    agg_res = cursor.fetchall()
    return {
        "date": datestr,
        "table": tblname,
        "nsynced": agg_res[0][0] if len(agg_res) > 0 else 0,
    }


# == start
def main():
    """main"""
    report = []
    with open(os.getenv("WAREHOUSESFILE"), "r", encoding="utf-8") as warehouses_file:
        warehouses = safe_load(warehouses_file)
        for orgname, warehouse in warehouses.items():
            with get_conn(warehouse) as conn:
                logger.info("fetching tables for %s", warehouse)
                tablenames = get_tables_in_schema(conn, warehouse)
                for tablename in tablenames:
                    stat = get_sync_counts_from_table(
                        conn, warehouse, tablename, report_date
                    )
                    stat["org"] = orgname
                    report.append(stat)

    with get_conn(
        {
            "HOST": os.getenv("ANALYTICSDBHOST"),
            "NAME": os.getenv("ANALYTICSDBNAME"),
            "USER": os.getenv("ANALYTICSDBUSER"),
            "PASS": os.getenv("ANALYTICSDBPASS"),
        }
    ) as analytics_conn:
        cursor = analytics_conn.cursor()
        for reportline in report:
            execute = f"""INSERT INTO syncstats ("org", "date", "table", "nsynced")
                VALUES (
                    '{reportline['org']}',
                    '{reportline['date']}',
                    '{reportline['table']}',
                    '{reportline['nsynced']}'
                )"""
            print(execute)
            try:
                cursor.execute(execute)
            except psycopg2.errors.ExclusionViolation:
                analytics_conn.rollback()
                execute = f"""UPDATE syncstats
                SET "nsynced" = '{reportline['nsynced']}'
                WHERE "org" = '{reportline['org']}'
                    AND "date" = '{reportline['date']}'
                    AND "table" = '{reportline['table']}'
                """
                cursor.execute(execute)


main()
