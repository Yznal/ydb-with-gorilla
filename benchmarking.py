# SDK usage is based on the example presented on:
# https://github.com/ydb-platform/ydb-python-sdk/blob/main/examples/basic_example_v1/basic_example.py
#
# TimeSeries usage example is presented on corresponding example:
# https://github.com/ydb-platform/ydb-python-sdk/tree/main/examples/time-series-serverless
#
# Read about YDB column-oriented tables specificity:
# https://ydb.tech/docs/en/concepts/datamodel/table#olap-data-types
#
# Read about YDB data types:
# https://ydb.tech/docs/en/yql/reference/types/primitive

import logging
import posixpath
import ydb

# Absolute path. Prefix to the table's names, including the database location.
DATABASE_PATH = "/Root/test/"
CONNECTION_TIMEOUT = 5
ENDPOINT = "grpc://localhost:2136"

# TODO:
#  * fix id from Uint64 to DATE
#  * fix data from Utf8 to Int8
#
# CREATE TABLE time_series_table (time DATE NOT NULL, data Int8, PRIMARY KEY (time)) WITH (STORE = COLUMN);
# [BULK] UPSERT INTO data(time, data) values (DateTime::MakeDate(Datetime("2019-01-01T15:30:00Z")), 1);
# SELECT PartIdx, RowCount, DataSize from `.sys/partition_stats` WHERE Path = "/Root/test/my_time_series_column";
TIME_SERIES_TABLE_NAME = "time_series_table"
TIME_SERIES_ID_COLUMN_NAME = "time"
TIME_SERIES_DATA_COLUMN_NAME = "data"
TIME_SERIES_DATA_COLUMN_VAR_NAME = "timeSeriesData"

BULK_UPSERT_QUANTITY = 1000
BULK_UPSERT_STRING_SIZE = 1000
total_data_size = 0


class Data(object):
    __slots__ = (TIME_SERIES_ID_COLUMN_NAME, TIME_SERIES_DATA_COLUMN_NAME)

    def __init__(self, time, data):
        self.time = time
        self.data = data


def get_data_for_bulk_upsert():
    return [Data(i, "*" * BULK_UPSERT_STRING_SIZE)
            for i in range(BULK_UPSERT_QUANTITY)]


def create_tables(pool, path):
    def create_tables_callee(session):
        session.execute_scheme(
            f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE TABLE IF NOT EXISTS `{TIME_SERIES_TABLE_NAME}` (
                `{TIME_SERIES_ID_COLUMN_NAME}` DATE NOT NULL,
                `{TIME_SERIES_DATA_COLUMN_NAME}` Int8,
                PRIMARY KEY (`{TIME_SERIES_ID_COLUMN_NAME}`)
            ) WITH (STORE = COLUMN)
            """
        )

    return pool.retry_operation_sync(create_tables_callee)


# NOTE: somewhy it doesn't work for table with column engine.
def select(pool, path):
    def callee(session):
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            PRAGMA TablePathPrefix("{path}");
            SELECT
                {TIME_SERIES_ID_COLUMN_NAME},
                {TIME_SERIES_DATA_COLUMN_NAME},
            FROM {TIME_SERIES_TABLE_NAME}
            """,
            commit_tx=True,
        )
        print("\n> select transaction result:")
        for row in result_sets[0].rows:
            print(
                "id: ",
                row.id,
                ", data: ",
                row.data,
            )

        return result_sets[0]

    return pool.retry_operation_sync(callee)


def select_stats(pool, path):
    def callee(session):
        global total_data_size

        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            PRAGMA TablePathPrefix("{path}");
            SELECT
                PartIdx,
                RowCount,
                DataSize,
            FROM `.sys/partition_stats`
            WHERE Path = "{path}{TIME_SERIES_TABLE_NAME}"
            """,
            commit_tx=True,
        )
        print("\n> select transaction result:")
        for row in result_sets[0].rows:
            print(
                "path_idx: ",
                row.PartIdx,
                ", row_count: ",
                row.RowCount,
                ", data_size: ",
                row.DataSize
            )
            total_data_size += row.DataSize

        return result_sets[0]

    return pool.retry_operation_sync(callee)


# NOTE: doesn't work for table with column engine.
def upsert(pool, path):
    def callee(session):
        session.transaction().execute(
            f"""
            PRAGMA TablePathPrefix("{path}");
            UPSERT INTO {TIME_SERIES_TABLE_NAME} ({TIME_SERIES_ID_COLUMN_NAME}, {TIME_SERIES_DATA_COLUMN_NAME}) VALUES
                (1, "DATA");
            """,
            commit_tx=True,
        )

    return pool.retry_operation_sync(callee)


def fill_tables(table_client, path):
    print(f"\n> bulk upsert: {TIME_SERIES_TABLE_NAME}")
    column_types_shortened = (
        ydb.BulkUpsertColumns()
        .add_column(TIME_SERIES_ID_COLUMN_NAME, ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column(TIME_SERIES_DATA_COLUMN_NAME, ydb.OptionalType(ydb.PrimitiveType.Utf8))
    )
    rows_to_upsert = get_data_for_bulk_upsert()
    table_client.bulk_upsert(
        posixpath.join(path, TIME_SERIES_TABLE_NAME),
        rows_to_upsert,
        column_types_shortened
    )


def describe_tables(pool, path, name):
    def callee(session):
        result = session.describe_table(posixpath.join(path, name))
        print(f"\n> describe table: {TIME_SERIES_TABLE_NAME}")
        for column in result.columns:
            print("column, name:", column.name, ",", str(column.type).strip())

    return pool.retry_operation_sync(callee)


def is_directory_exists(driver, path):
    try:
        return driver.scheme_client.describe_path(path).is_directory()
    except ydb.SchemeError:
        return False


def ensure_path_exists(driver, database, path):
    paths_to_create = list()
    path = path.rstrip("/")
    while path not in ("", database):
        full_path = posixpath.join(database, path)
        if is_directory_exists(driver, full_path):
            break
        paths_to_create.append(full_path)
        path = posixpath.dirname(path).rstrip("/")

    while len(paths_to_create) > 0:
        full_path = paths_to_create.pop(-1)
        driver.scheme_client.make_directory(full_path)


def run(endpoint, database_path):
    driver_config = ydb.DriverConfig(endpoint, database_path)
    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=CONNECTION_TIMEOUT, fail_fast=True)

        with ydb.SessionPool(driver) as pool:
            create_tables(pool, database_path)
            describe_tables(pool, database_path, TIME_SERIES_TABLE_NAME)
            fill_tables(driver.table_client, database_path)
            select_stats(pool, database_path)
            print(f"Total data size is: {total_data_size}")


if __name__ == "__main__":
    verbose = False

    if verbose:
        logger = logging.getLogger("ydb.pool.Discovery")
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler())

    run(ENDPOINT, DATABASE_PATH)
