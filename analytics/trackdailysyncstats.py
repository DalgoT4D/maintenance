#!/usr/bin/env python3

import os
import argparse
import psycopg2
from yaml import safe_load
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--date")
args = parser.parse_args()

report_date = (
    datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.today()
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

    cursor.execute(
        f"""SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{warehouse['SCHEMA']}'
            AND table_catalog = '{warehouse["NAME"]}'
        """
    )

    results = cursor.fetchall()
    return [x[0] for x in results]


def get_sync_counts_from_table(
    conn, warehouse: dict, tblname: str, date: datetime
) -> dict:
    """reads the staging table to get records synced by date"""

    cursor = conn.cursor()
    datestr = date.strftime("%Y-%m-%d")

    cursor.execute(
        f"""SELECT count(1) 
        FROM {warehouse['SCHEMA']}.{tblname} 
        WHERE DATE(_airbyte_emitted_at) = '{datestr}'
        """
    )

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
    with open("warehouses.yaml", "r", encoding="utf-8") as warehouses_file:
        warehouses = safe_load(warehouses_file)
        for orgname, warehouse in warehouses.items():
            with get_conn(warehouse) as conn:
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
            execute = f"""INSERT INTO syncstats (org, date, table, nsynced)
                VALUES (
                    '{reportline['org']}',
                    '{reportline['date']}',
                    '{reportline['table']}',
                    '{reportline['nsynced']}'
                )"""
            print(execute)
            cursor.execute(execute)


main()
