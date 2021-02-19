#!/usr/bin/env python

from os import getenv
import yaml
import json
import csv
import pymssql
import logging
import pandas as pd
from pathlib import Path
from tempfile import NamedTemporaryFile
from contextlib import closing
from dataclasses import dataclass, field, asdict, astuple
from typing import Generator, Tuple, List, Union
from textwrap import dedent

logger = logging.getLogger(__name__)

# size of batches for query download
BATCH_SIZE = 500000


@dataclass()
class MSConfig:
    """ Connection to MSSQL """

    server: str = ""
    user: str = ""
    password: str = ""
    port: int = 0
    appname: str = ""

    def __post_init__(self):
        self.server = getenv("FN_SQL_SERVER")
        self.user = getenv("FN_SQL_USER")
        self.password = getenv("FN_SQL_PASSWORD")
        self.port = getenv("FN_SQL_PORT", 1433)
        self.appname = "flakenews"


@dataclass()
class Column:
    """
    Part of TableRules Class
    """

    name: str
    ordinal_position: int
    data_type: str
    numeric_precision: int
    numeric_scale: int

    def clean_name(self):
        return self.name.replace(" ", "_")
    def caps_name(self):
        return self.clean_name().upper()


@dataclass()
class Table:
    """
    Part of TableRules Class
    name: of the table
    pk: list of 1 or more columns comprising the primary key of the table
    cols: ordered list of the columns in the table
    row_split_size: number of rows to query and upload to destination - for huge tables

    """

    name: str
    cols: List[str] = field(default_factory=list)
    pk: List[str] = field(default_factory=list)
    row_split_size: int = 500000
    sf_ddl: str = ""

    def __post_init__(self):
        if self.cols:
            self.cols = [Column(**col) for col in self.cols]


@dataclass
class Schema:
    """ Part of TableRules Class """

    name: str
    tables: List[Table]

    def __post_init__(self):
        self.tables = [Table(**table) for table in self.tables]


@dataclass
class Database:
    """ Part of TableRules Class """

    name: str
    schemas: List[Schema]

    def __post_init__(self):
        self.schemas = [Schema(**schema) for schema in self.schemas]


@dataclass
class TableRules:
    """
    Initialized with basic table_config.yml
    Later enhanced with metadata from the source database(s) to fill out Table Rules
    """

    databases: List[Database]

    def __post_init__(self):
        self.databases = [Database(**database) for database in self.databases]

    def _all_tables(self) -> Tuple[Database, Schema, Table]:
        """
        Iterates through all of the 3-part-namespaces that uniquely identify tables.
        """
        for db in self.databases:
            for sch in db.schemas:
                for tbl in sch.tables:
                    yield db, sch, tbl

    def _pk_sql(self) -> str:
        """
        Return a metadata query to get the primary keys for tables
        """

        qry = """
            ;with pks as ("""
        qry += """

                union all
            """.join(
            f"""
                select
                    lower(constr.constraint_catalog) collate sql_latin1_general_cp1_ci_as as table_catalog
                    , lower(constr.constraint_schema) collate sql_latin1_general_cp1_ci_as as table_schema
                    , lower(kcu.table_name) collate sql_latin1_general_cp1_ci_as as table_name
                    , lower(kcu.column_name) collate sql_latin1_general_cp1_ci_as as column_name
                    , kcu.ordinal_position
                from [{db.name}].[information_schema].[table_constraints] as constr
                join [{db.name}].[information_schema].[key_column_usage] as kcu
                    on constr.constraint_name = kcu.constraint_name
                where constraint_type = 'primary key'"""
            for db in self.databases
        )
        qry += """
            )    
            select
                table_catalog
                , table_schema
                , table_name
                , stuff(
                    (select 
                        ',' + pk1.column_name
                    from pks as pk1
                    where pk1.table_catalog = pk2.table_catalog
                        and  pk1.table_schema = pk2.table_schema
                        and pk1.table_name = pk2.table_name
                    order by pk1.ordinal_position
                    for xml path(''), type
                    ).value('.', 'varchar(max)'), 1, 1, ''
                ) as primary_key_columns
            from pks as pk2
            group by
                pk2.table_catalog
                , pk2.table_schema
                , pk2.table_name
            """
        qry = dedent(qry)
        logger.debug(qry)
        return qry

    def _cols_sql(self):
        """
        Return an ordered metadata query to get the columns, their types, and their order within tables
        """

        qry = """
            ;with cols as ("""
        qry += """

                union all
            """.join(
            f"""
            select 
                lower(table_catalog) collate sql_latin1_general_cp1_ci_as as table_catalog 
                , lower(table_schema) collate sql_latin1_general_cp1_ci_as as table_schema
                , lower(table_name) collate sql_latin1_general_cp1_ci_as as table_name
                , lower(column_name) collate sql_latin1_general_cp1_ci_as as column_name
                , ordinal_position
                , lower(data_type) collate sql_latin1_general_cp1_ci_as as data_type
                , numeric_precision
                , numeric_scale
            from [{db.name}].information_schema.columns"""
            for db in self.databases
        )
        qry += """
            )    
            select
                table_catalog
                , table_schema
                , table_name
                , column_name
                , ordinal_position
                , data_type
                , numeric_precision
                , numeric_scale
            from cols
            order by table_catalog, table_schema, table_name, ordinal_position
            """
        qry = dedent(qry)
        logger.debug(qry)
        return qry

    def _match_results_to_tables(self, cur: pymssql.Cursor, qry: str) -> None:

        cur.execute(qry)
        results = cur.fetchall()

        for row in results:
            for db, sch, tbl in self._all_tables():
                if row[0] == db.name and row[1] == sch.name and row[2] == tbl.name:
                    yield row, tbl

    def _set_primary_keys(self, cur: pymssql.Cursor) -> None:
        """
        Enriches this TableRules instance with Primary Keys for each table

        Requires a pymssql.Cursor
        """
        qry = self._pk_sql()
        for row, tbl in self._match_results_to_tables(cur, qry):
            tbl.pk = row[3].split(",")
            break

    def _set_cols(self, cur: pymssql.Cursor) -> None:
        """
        Enriches this TableRules instance with Column metadata for each table,
        including name, datatype, order, precision and scale.

        Requires a pymssql.Cursor
        """

        qry = self._cols_sql()

        for row, tbl in self._match_results_to_tables(cur, qry):
            tbl.cols.append(
                Column(
                    name=row[3],
                    ordinal_position=row[4],
                    data_type=row[5],
                    numeric_precision=row[6],
                    numeric_scale=row[7],
                )
            )

    def _check_metadata(self) -> str:
        table_errors = ""
        for db, sch, tbl in self._all_tables():
            if not tbl.cols:
                table_errors += (
                    f"\nTable not found, or no permission: {db.name}.{sch.name}.{tbl.name}"
                )
        if table_errors:
            table_errors = "\nCheck your config or SQL Server permissions\n" + table_errors
            logger.error(table_errors)

    def _map_datatype(self, col: Column) -> str:
        """
        Map a SQL Server data type to the Snowflake equivalent
        """

        def _qualify_precision_and_scale(col: Column) -> str:
            return (
                "number("
                + str(col.numeric_precision)
                + ", "
                + str(col.numeric_scale)
                + ")"
            )

        type_mapping = {
            "varchar": "varchar",
            "char": "varchar",
            "nvarchar": "varchar",
            "nchar": "varchar",
            "ntext": "varchar",
            "text": "varchar",
            "uniqueidentifier": "varchar",
            "bigint": "number",
            "int": "number",
            "smallint": "number",
            "tinyint": "number",
            "float": col.data_type,
            "real": "float",
            "numeric": _qualify_precision_and_scale(col),
            "decimal": _qualify_precision_and_scale(col),
            "money": _qualify_precision_and_scale(col),
            "smallmoney": _qualify_precision_and_scale(col),
            "bit": "boolean",
            "datetime": "timestamp",
            "datetime2": "timestamp",
            "smalldatetime": "timestamp",
            "time": col.data_type,
            "date": col.data_type,
            "varbinary": "binary",
            "binary": "variant", #col.data_type, ideally these should be binary but sql -> pandas -> snowflake doesn't like it
            "rowversion": "variant", #"binary",
            "image": "variant", #"binary",
        }
        return type_mapping.get(col.data_type, "variant")

    def _set_sf_ddl(self, use_pk: bool = True):
        """
        Generates a `create or replace table` statement valid for snowflake
        Expects the cols to be set already on the Table
        """

        def _with_pk(use_pk: bool, tbl: Table) -> str:
            if use_pk and tbl.pk:
                return (
                    "    , constraint pk_"
                    + tbl.name
                    + " primary key ("
                    + ", ".join(tbl.pk)
                    + ")\n"
                )
            return ""

        for _, _, tbl in self._all_tables():
            tbl.sf_ddl = (
                "create or replace table "
                + tbl.name
                + " (\n    "
                + "    , ".join(
                    col.clean_name() + " " + self._map_datatype(col) + "\n"
                    for col in tbl.cols
                )
                + _with_pk(use_pk, tbl)
                + ");"
            )

    def set_tables_metadata(self, conn: pymssql.Connection) -> None:
        """
        Enriches this TableRules instance with Primary Key and Column metadata for each table

        Requires a pymssql.Connection handle
        """
        with closing(conn.cursor()) as cur:
            self._set_primary_keys(cur)
            self._set_cols(cur)
            self._check_metadata()
            self._set_sf_ddl()

    def output_ddl(self, f: Path) -> None:
        with open(f, "w") as f:
            for _, _, tbl in self._all_tables():
                if not tbl.sf_ddl:
                    raise ValueError("The Snowflake DDL has not been set")
                f.write(tbl.sf_ddl + "\n\n")

    def output_rules(self, f: Path) -> None:
        with open(f, "w") as f:
            d = asdict(self)
            logger.debug(d)
            doc = json.dump(d, f, indent=2)

    def get_basic_sql(self) -> Generator[Tuple, None, None]:
        for db, sch, tbl in self._all_tables():
            yield db, sch, tbl, f"""select {",".join(["[" + col.name + "]" for col in tbl.cols])} from [{db.name}].[{sch.name}].[{tbl.name}]"""


def new_table_rules_from_config(infile: str) -> TableRules:
    """
    Constructs a TableRules object stub from a table_config.yml file
    The infile expects a specific format.
    """
    with open(infile, "r") as stream:
        try:
            table_config = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print(e)
            sys.exit(1)
        rules = TableRules(table_config.get("databases"))
        logger.debug(rules)
        return rules


def new_table_rules(infile: str) -> TableRules:
    """
    Constructs a TableRules object stub from a table_rules.json file
    The infile expects a specific format.
    """
    with open(infile, "r") as stream:
        try:
            table_config = json.load(stream)
        except json.JSONDecodeError as e:
            print(e)
            sys.exit(1)
        rules = TableRules(table_config.get("databases"))
        logger.debug(rules)
        return rules


def new_conn(config: Union[MSConfig, None] = None) -> pymssql.Connection:
    """
    Creates a DBAPI connection object for MSSQL Server
    """
    if config is None:
        config = MSConfig()

    return pymssql.connect(**asdict(config))


def get_batch(
    cur: pymssql.Cursor, batch_size: int = BATCH_SIZE
) -> Generator[Tuple, None, None]:
    """
    Generate batches of BATCH_SIZE rows.
        Would have been nice to use cursor.rownumber,
        but it does not increment; it is stuck at -1
        due to this: https://github.com/pymssql/pymssql/issues/141

        So, the offset for the final batch is probably overestimated,
        at least it provides a unique file-split name.
    """
    offset = 0
    while True:
        batch = cur.fetchmany(BATCH_SIZE)
        if not batch:
            break
        offset += BATCH_SIZE
        yield batch, offset


def fix_date_cols(df: pd.DataFrame, tz: str = 'UTC') -> None:
    """
    Workaround adds timezone to timestamps for this issue with datetimes
    https://github.com/snowflakedb/snowflake-connector-python/issues/319
    """
    cols = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in cols:
        df[col] = df[col].dt.tz_localize(tz)


def to_pandas(rules: TableRules, conn: pymssql.Connection):
    """
    Batch out the data into pandas dataframes
    TODO: Consider specifying the dt_types() in the pandas constructor
          so that datetime64[ns] has UTC by default.
          That would require a full SQL -> pd dtype mapping
    """
    with closing(conn.cursor()) as cur:
        for db, sch, tbl, qry in rules.get_basic_sql():
            cur.execute(qry)
            logger.info(f"Querying table: {db.name}.{sch.name}.{tbl.name}")

            for batch, rownum in get_batch(cur):
                df = pd.DataFrame(
                    data=batch, columns=[col.caps_name() for col in tbl.cols]
                )
                logger.info(df.head(1))
                fix_date_cols(df)
                yield db, sch, tbl, df


def write_parquet(rules: TableRules, conn: pymssql.Connection):
    """
    Batch out the data into parquet files

    Consider a buffer and passing that to boto3 s3
    import io
    f = io.BytesIO()
    df.to_parquet(f)
    f.seek(0)
    content = f.read()
    """
    with closing(conn.cursor()) as cur:
        for db, sch, tbl, qry in rules.get_basic_sql():
            cur.execute(qry)
            logger.info(f"Querying table: {db.name}.{sch.name}.{tbl.name}")

            for batch, rownum in get_batch(cur):
                df = pd.DataFrame(
                    data=batch, columns=[col.caps_name() for col in tbl.cols]
                )
                logger.info(df.head(1))
                path = Path(f"./temp/{tbl.name}_{str(rownum)}.parquet.gzip")
                df.to_parquet(path, compression="gzip")
                logger.info(f"File created: {path.resolve()}")
                yield db, sch, tbl, path

def head_parquet(f: Path) -> None:
    """
    A function to inspect a parquet file
    TODO: Move this into tests
    """
    df = pd.read_parquet(f)
    logger.info(df.head())
    logger.info(df.dtypes)
    logger.info(df.info(verbose=True))