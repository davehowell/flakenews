#!/usr/bin/env python

from os import getenv
import logging
from typing import Union
from dataclasses import dataclass, asdict
import snowflake.connector as snowflake
from snowflake.connector.network import DEFAULT_AUTHENTICATOR
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from .mssql import Table

logger = logging.getLogger(__name__)


@dataclass()
class SFConfig:
    """ Connection to Snowflake """

    account: str = ""
    user: str = ""
    password: str = ""
    authenticator: str = DEFAULT_AUTHENTICATOR
    role: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""

    def __post_init__(self):
        self.account = getenv("FN_SF_ACCOUNT")
        self.user = getenv("FN_SF_USER")
        self.password = getenv("FN_SF_PASSWORD")
        self.authenticator = getenv("FN_SF_AUTHENTICATOR", DEFAULT_AUTHENTICATOR)
        self.role = getenv("FN_SF_ROLE")
        self.warehouse = getenv("FN_SF_WAREHOUSE")
        self.database = getenv("FN_SF_DATABASE")
        self.schema = getenv("FN_SF_SCHEMA")


def new_conn(config: Union[SFConfig, None] = None) -> snowflake.SnowflakeConnection:
    """
    Creates a DBAPI connection object for Snowflake
    """
    if config is None:
        config = SFConfig()

    return snowflake.connect(**asdict(config))


def write_df(
    df: pd.DataFrame, tbl: Table, conn: snowflake.SnowflakeConnection = None
) -> bool:
    """ Thin wrapper over snowflake.write_pandas function """
    logger.info(f"Copying data to table {tbl.name} on Snowflake")
    success, nchunks, nrows, output = write_pandas(conn, df, tbl.name.upper())
    logger.info(
        f"""{"Succeeded" if success else "Failed"}: chunks {nchunks}, rows {nrows}, output {output}"""
    )
    return success
