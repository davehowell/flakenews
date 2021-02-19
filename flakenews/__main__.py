#!/usr/bin/env python
import sys
import logging
import argparse
from contextlib import closing
from . import mssql
from . import snowflake as sf


APP_NAME = "FlakeNews"


def run_parquet_files(rules_file: str) -> None:
    """
    A WIP function that writes parquet files locally
    TODO: files to s3, cleanup local files
    """
    rules = mssql.new_table_rules(rules_file)
    with closing(mssql.new_conn()) as conn:
        for db, sch, tbl, path in mssql.write_parquet(rules, conn):
            pass


def run_to_snowflake(rules_file: str) -> None:
    rules = mssql.new_table_rules(rules_file)
    with closing(mssql.new_conn()) as ms_conn, closing(sf.new_conn()) as sf_conn:
        for db, sch, tbl, df in mssql.to_pandas(rules, ms_conn):
            sf.write_df(df, tbl, sf_conn)


def run_rules_setup(config_file: str) -> None:
    """
    Using a basic list of tables from table_config.yml, enrich with metadata from
    the source database and output a detailed table_rules.yml file and table_ddl.sql file.
    """
    rules = mssql.new_table_rules_from_config(config_file)
    with closing(mssql.new_conn()) as conn:
        rules.set_tables_metadata(conn)

    rules.output_ddl("./table_ddl.sql")
    rules.output_rules("./table_rules.json")


def main(args: argparse.Namespace):
    if args.table_config:
        run_rules_setup(args.table_config)
    if args.table_rules:
        run_to_snowflake(args.table_rules)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        help="default is INFO, other options include DEBUG, WARNING, ERROR, CRITICAL",
    )
    parser.add_argument(
        "-c",
        "--table-config",
        help="e.g. table_config.yml, cannot be used with --table-rules",
    )
    parser.add_argument(
        "-r",
        "--table-rules",
        help="e.g. table_rules.json, cannot be used with --table-config",
    )
    args = parser.parse_args()

    try:
        if logging.getLogger().hasHandlers():
            logging.getLogger().setLevel(args.log_level)
        else:
            logging.basicConfig(level=args.log_level)
    except ValueError:
        logging.error(f"\n\nInvalid log level: {args.log_level}\n")
        parser.print_help(sys.stderr)
        sys.exit(1)

    if bool(args.table_config) == bool(args.table_rules):
        logging.error(
            "\n\nProvide either --table-config or --table-rules and not both\n"
        )
        parser.print_help(sys.stderr)
        sys.exit(1)

    main(args)
