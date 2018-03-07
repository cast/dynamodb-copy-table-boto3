import sys
import os
from time import sleep
import boto3
import multiprocessing
import itertools

spinner = itertools.cycle(['-', '/', '|', '\\'])

localDynamoHost='http://192.168.99.100:8000'


def copy_items(src_table, src_client, dst_table, dst_client, segment, total_segments):
    # copy over item
    item_count = 0
    paginator = src_client.get_paginator('scan')

    for page in paginator.paginate(
            TableName=src_table,
            Select='ALL_ATTRIBUTES',
            ReturnConsumedCapacity='NONE',
            ConsistentRead=True,
            Segment=segment,
            TotalSegments=total_segments,
            PaginationConfig={"PageSize": 25}):

        batch = []
        for item in page['Items']:
            item_count += 1
            batch.append({
                'PutRequest': {
                    'Item': item
                }
            })

        print("Process {0} put {1} items".format(segment, item_count))
        dst_client.batch_write_item(
            RequestItems={
               dst_table: batch
            }
        )


def check_tables(src_table, src_client, dst_table, dst_client):
    # get source table and its schema
    print("Describe table '" + src_table + "'")
    try:
        src_schema = src_client.describe_table(TableName=src_table)["Table"]
    except src_client.exceptions.ResourceNotFoundException:
        print("!!! Table {0} does not exist. Exiting...".format(src_table))
        sys.exit(1)
    except:
        print("!!! Error reading table {0} . Exiting...".format(src_table))
        sys.exit(1)

    # get destination table and its schema
    print("Describe table '" + dst_table + "'")
    try:
        dest_schema = dst_client.describe_table(TableName=dst_table)["Table"]
    except dst_client.exceptions.ResourceNotFoundException:
        print("!!! Table {0} does not exist. Exiting...".format(src_table))
        sys.exit(1)
    except:
        print("!!! Error reading table {0} . Exiting...".format(src_table))
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: {0} <source_table_name> <source_region> <destination_table_name> <destination_region>".format(sys.argv[0]))
        sys.exit(1)

    table_1 = sys.argv[1]
    table_1_region = sys.argv[2]
    table_2 = sys.argv[3]
    table_2_region = sys.argv[4]

    db_client_1 = boto3.client('dynamodb', region_name=table_1_region)
    db_client_2 = boto3.client('dynamodb', region_name=table_2_region)

    check_tables(table_1, db_client_1, table_2, db_client_2)

    pool_size = 4  # tested with 4, took 5 minutes to copy 150,000+ items
    pool = []

    for i in range(pool_size):
        worker = multiprocessing.Process(
            target=copy_items,
            kwargs={
                'src_table': table_1,
                'src_client': db_client_1,
                'dst_table': table_2,
                'dst_client': db_client_2,
                'segment': i,
                'total_segments': pool_size
            }
        )
        pool.append(worker)
        worker.start()

    for process in pool:
        process.join()

    print("*** All Jobs Done. Exiting... ***")
