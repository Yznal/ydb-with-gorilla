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
import datetime
import logging
import os
import posixpath
import time

import ydb
import pandas as pd

# Flag indicating whether we're working with patched version of YDB
# with Gorilla being used.
IS_PATCHED_VERSION = True
OUTPUT_CSV_FILE_NAME_SIZE = "bench_results_size.csv"
OUTPUT_CSV_FILE_NAME_TIME_UPSERT = "bench_results_insert.csv"
OUTPUT_CSV_FILE_NAME_TIME_SELECT = "bench_results_select.csv"

# Absolute path. Prefix to the table's names, including the database location.
DATABASE_PATH = "/Root/test/"
CONNECTION_TIMEOUT = 5
ENDPOINT = "grpc://localhost:2136"

# CREATE TABLE time_series_table (time Timestamp NOT NULL, data Uint8, PRIMARY KEY (time)) WITH (STORE = COLUMN);
# [BULK] UPSERT INTO data(time, data) values (DateTime::MakeDate(Datetime("2019-01-01T15:30:00Z")), 1);
# SELECT PartIdx, RowCount, DataSize from `.sys/partition_stats` WHERE Path = "/Root/test/my_time_series_column";
TIME_SERIES_TABLE_NAME = "time_series_table"
TIME_SERIES_ID_COLUMN_NAME = "time"
TIME_SERIES_DATA_COLUMN_NAME = "data"
TIME_SERIES_DATA_COLUMN_VAR_NAME = "timeSeriesData"

BULK_UPSERT_QUANTITY = 100000
BULK_UPSERT_UINT_VALUE = 42


# Helper class representing data we're going to
# bulk upsert into the test table.
class Data(object):
    __slots__ = (TIME_SERIES_ID_COLUMN_NAME, TIME_SERIES_DATA_COLUMN_NAME)

    def __init__(self, time, data):
        self.time = time
        self.data = data


def get_data_for_bulk_upsert():
    return [Data(i, BULK_UPSERT_UINT_VALUE) for i in range(BULK_UPSERT_QUANTITY)]


def create_tables(pool, path):
    def create_tables_callee(session):
        session.execute_scheme(
            f"""
            PRAGMA TablePathPrefix("{path}");
            CREATE TABLE `{TIME_SERIES_TABLE_NAME}` (
                `{TIME_SERIES_ID_COLUMN_NAME}` Timestamp NOT NULL,
                `{TIME_SERIES_DATA_COLUMN_NAME}` Uint8,
                PRIMARY KEY (`{TIME_SERIES_ID_COLUMN_NAME}`)
            ) WITH (STORE = COLUMN)
            """
        )

    return pool.retry_operation_sync(create_tables_callee)


# NOTE: Doesn't work for table with column engine.
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
        print("> Select result:")
        for row in result_sets[0].rows:
            print(f"ROW. {TIME_SERIES_ID_COLUMN_NAME}: {row.id}, {TIME_SERIES_DATA_COLUMN_NAME}: {row.data}")

        return result_sets[0]

    return pool.retry_operation_sync(callee)


# Variable to store total data size resulted from bulk upsert.
total_data_size = 0


def select_stats(pool, path):
    def callee(session):
        global total_data_size

        print(f"Path is: {path}{TIME_SERIES_TABLE_NAME}")

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
        print("> Stats:")
        for row in result_sets[0].rows:
            print(f"path_idx: {row.PartIdx}, row_count: {row.RowCount}, data_size: {row.DataSize}")
            total_data_size += row.DataSize
        return result_sets[0]

    return pool.retry_operation_sync(callee)


# NOTE: Doesn't work for table with column engine.
def upsert(pool, path):
    def callee(session):
        session.transaction().execute(
            f"""
            PRAGMA TablePathPrefix("{path}");
            UPSERT INTO
            {TIME_SERIES_TABLE_NAME} ({TIME_SERIES_ID_COLUMN_NAME}, {TIME_SERIES_DATA_COLUMN_NAME})
            VALUES (..., ...);
            """,
            commit_tx=True,
        )

    return pool.retry_operation_sync(callee)


def fill_tables(table_client, path):
    print(f"> Bulk upsert into {TIME_SERIES_TABLE_NAME}")
    column_types_shortened = (
        ydb.BulkUpsertColumns()
        .add_column(TIME_SERIES_ID_COLUMN_NAME, ydb.OptionalType(ydb.PrimitiveType.Timestamp))
        .add_column(TIME_SERIES_DATA_COLUMN_NAME, ydb.OptionalType(ydb.PrimitiveType.Uint8))
    )
    rows_to_upsert = get_data_for_bulk_upsert()
    table_client.bulk_upsert(
        posixpath.join(path, TIME_SERIES_TABLE_NAME),
        rows_to_upsert,
        column_types_shortened
    )


def drop(pool, path):
    def create_tables_callee(session):
        session.execute_scheme(
            f"""
            PRAGMA TablePathPrefix("{path}");
            DROP TABLE `{TIME_SERIES_TABLE_NAME}`
            """
        )

    return pool.retry_operation_sync(create_tables_callee)


def describe_tables(pool, path, name):
    def callee(session):
        result = session.describe_table(posixpath.join(path, name))
        print(f"> Describe table: {TIME_SERIES_TABLE_NAME}")
        for column in result.columns:
            print(f"Column: {column.name}, {str(column.type).strip()}")

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


def measure_execution_time_seconds(lambd):
    time_start = time.time()
    lambd()
    time_end = time.time()
    time_elapsed_seconds = time_end - time_start
    return time_elapsed_seconds


def run(endpoint, database_path):
    driver_config = ydb.DriverConfig(endpoint, database_path)
    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=CONNECTION_TIMEOUT, fail_fast=True)

        # IsPatched, RowsNumber, TotalDataSize
        bench_data_size = []
        # IsPatched, RowsNumber, TotalUpsertTime
        bench_time_upsert = []
        # IsPatched, RowsNumber, TotalSelectTime
        bench_time_select = []

        with ydb.SessionPool(driver) as pool:
            create_tables(pool, database_path)
            describe_tables(pool, database_path, TIME_SERIES_TABLE_NAME)

            time_upsert = measure_execution_time_seconds(lambda: fill_tables(driver.table_client, database_path))
            time.sleep(2)
            select_stats(pool, database_path)
            print(f"Total inserted data size is: {total_data_size}")

            bench_time_upsert.append([IS_PATCHED_VERSION, BULK_UPSERT_QUANTITY, time_upsert])
            bench_data_size.append([IS_PATCHED_VERSION, BULK_UPSERT_QUANTITY, total_data_size])

        # df_size = pd.DataFrame(bench_data_size)
        # df_size.to_csv(OUTPUT_CSV_FILE_NAME_SIZE, mode='a', header=not os.path.exists(OUTPUT_CSV_FILE_NAME_SIZE))
        #
        # df_upsert = pd.DataFrame(bench_time_upsert)
        # df_upsert.to_csv(OUTPUT_CSV_FILE_NAME_TIME_UPSERT, mode='a',
        #                  header=not os.path.exists(OUTPUT_CSV_FILE_NAME_TIME_UPSERT))
        #
        # df_select = pd.DataFrame(bench_time_select)
        # df_select.to_csv(OUTPUT_CSV_FILE_NAME_TIME_SELECT, mode='a',
        #                  header=not os.path.exists(OUTPUT_CSV_FILE_NAME_TIME_SELECT))


def run_interactive(endpoint, database_path):
    global total_data_size

    driver_config = ydb.DriverConfig(endpoint, database_path)
    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=CONNECTION_TIMEOUT, fail_fast=True)
        with ydb.SessionPool(driver) as pool:
            last_command = None
            while last_command is None or last_command != "exit":
                last_command = input("> ")
                match last_command:
                    case "upsert":
                        fill_tables(driver.table_client, database_path)
                    case "create":
                        create_tables(pool, database_path)
                    case "describe":
                        describe_tables(pool, database_path, TIME_SERIES_TABLE_NAME)
                    case "stats":
                        select_stats(pool, database_path)
                        print(f"Total inserted data size is: {total_data_size}")
                        total_data_size = 0
                    case "drop":
                        drop(pool, database_path)
                    case _:
                        print("Unsupported command")


if __name__ == "__main__":
    verbose = False

    if verbose:
        logger = logging.getLogger("ydb.pool.Discovery")
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler())

    # run(ENDPOINT, DATABASE_PATH)
    run_interactive(ENDPOINT, DATABASE_PATH)

# Total inserted data size is: 481776 (without Gorilla)
# Total inserted data size is: 481776 with Gorilla
