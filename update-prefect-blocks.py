#!/usr/bin/env python3

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--block-name")
parser.add_argument("--command")
parser.add_argument("--project-dir")
parser.add_argument("--working-dir")
parser.add_argument("--profiles-dir")
args = parser.parse_args()

from prefect_dbt.cli.commands import DbtCoreOperation

if args.block_name:
    block: DbtCoreOperation = DbtCoreOperation.load(args.block_name)

    print(block.commands[0])
    print(block.project_dir)
    print(block.working_dir)
    print(block.profiles_dir)

    if args.command:
        block.commands = [args.command]
    if args.project_dir:
        block.project_dir = args.project_dir
    if args.working_dir:
        block.working_dir = args.working_dir
    if args.profiles_dir:
        block.profiles_dir = args.profiles_dir

    # save even if we don't update anything - this is useful when prefect releases schema changes
    block.save(args.block_name, overwrite=True)
