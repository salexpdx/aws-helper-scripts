#!/usr/local/bin/python3.6
# Python script that reviews dynamo tables to see if on-demand may be better
# from a pricing perspective.  This script is designed to help users make the
# decision.  Any suggestions are based on looking back and not into the future
# if your usage of the table changes, your pricing models should be reconsidered.

#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Dict, Union, Tuple

import boto3
import datetime
import statistics
import logging
import pprint
logging.basicConfig(level=logging.WARNING)
# grab our own logging namespace so we can put just it into debug for development
logger = logging.getLogger('analyze_dynamo_provisions.py')
# logger.setLevel(logging.INFO)

pp = pprint.PrettyPrinter(indent=4)

periods: Dict[str, int] = dict()
periods["hourly"] = 60 * 60
periods["daily"] = 60 * 60 * 24
periods["weekly"] = 60 * 60 * 24 * 7
periods["monthly"] = 60 * 60 * 720

dynamo_pricing = dict()
dynamo_pricing["provisioned_write"] = 0.00065
dynamo_pricing["provisioned_read"] = 0.00013
dynamo_pricing["on_demand_write"] = 1.25 / 1000000
dynamo_pricing["on_demand_read"] = .25 / 1000000
minimum_provisioned_price = dynamo_pricing["provisioned_write"] + dynamo_pricing["provisioned_read"]


def get_dynamo_table_metric(table_name, statistics, metric_name, period, unit, start_date, end_date):
    return cloudwatch_client.get_metric_statistics(
        Namespace="AWS/DynamoDB",
        MetricName=metric_name,
        Dimensions=[
            {
                'Name': "TableName",
                'Value': table_name
            }
        ],
        StartTime=datetime.datetime(start_date.year, start_date.month, start_date.day),
        EndTime=datetime.datetime(end_date.year, end_date.month, end_date.day),
        Period=period,
        Statistics=statistics,
        Unit=unit
    )


def calculate_on_demand_price(writes, reads):
    return (writes * dynamo_pricing["on_demand_write"]) + (reads * dynamo_pricing["on_demand_read"])


def calculate_provisioned_price(table_name):
    provisioned_reads = get_dynamo_table_metric(
            table_name,
            ["Average"],
            "ProvisionedReadCapacityUnits",
            periods["daily"],
            "Count",
            three_months_ago,
            yesterday
        )
    provisioned_read_list = []

    for metric_item in provisioned_reads["Datapoints"]:
        provisioned_read_list.append(metric_item["Average"])

    # the + .999 rounds up since provisions are integers.
    average_provisioned_reads = int(statistics.mean(provisioned_read_list) + .999)
    provisioned_read_price = average_provisioned_reads * dynamo_pricing["provisioned_write"] * 720

    provisioned_writes = get_dynamo_table_metric(
        table_name,
        ["Average"],
        "ProvisionedWriteCapacityUnits",
        periods["daily"],
        "Count",
        three_months_ago,
        yesterday
    )
    provisioned_write_list = []

    for metric_item in provisioned_writes["Datapoints"]:
        provisioned_write_list.append(metric_item["Average"])

    # the + .999 rounds up since provisions are integers.
    average_provisioned_writes = int(statistics.mean(provisioned_write_list) + .999)
    provisioned_write_price = average_provisioned_writes * dynamo_pricing["provisioned_write"] * 720

    return provisioned_read_price + provisioned_write_price


def get_table_pricing_model(table_details):
    read_capacity = table_details["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"]
    write_capacity = table_details["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"]
    if read_capacity == 0 and write_capacity == 0:
        return "on-demand"
    else:
        return "provisioned"

dynamo_client = boto3.client('dynamodb')
cloudwatch_client = boto3.client('cloudwatch')

# drop back 1 day to ensure we have complete statistics to look at vs half a day
yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
# now setup a few standard dates that we are going to use throughout the program.
week_ago = yesterday - datetime.timedelta(weeks=1)
# fuzzy here as to what is a month, but aws seems to build around 720 hour months (30 days)
month_ago = yesterday - datetime.timedelta(hours=720)
# three months would be 2160 = 720 * 3
three_months_ago = yesterday - datetime.timedelta(hours=2160)

logger.info(f'will use end date {yesterday.year} {yesterday.month} {yesterday.day}')
logger.info(f'week_ago is  {week_ago.year} {week_ago.month} {week_ago.day}')
logger.info(f'month_ago is  {month_ago.year} {month_ago.month} {month_ago.day}')
logger.info(f'three_months_ago is  {three_months_ago.year} {three_months_ago.month} {three_months_ago.day}')

# No reason to pull all the cloudwatch metrics for tables if they have already been
# deleted, start by grabbing the list of tables and then only look up details on them
tables_response = dynamo_client.list_tables()
tables = tables_response["TableNames"]

for table in tables:
    logger.info(f'looking up info {table}')

    table_response = dynamo_client.describe_table(TableName=table)

    pricing_model = get_table_pricing_model(table_response)

    write_metrics = get_dynamo_table_metric(
        table,
        ["Sum"],
        "ConsumedWriteCapacityUnits",
        periods["daily"],
        "Count",
        three_months_ago,
        yesterday
    )
    # logger.debug(pp.pformat(write_metrics))

    write_list = []
    sum_writes = 0
    sum_writes_month_one = 0
    sum_writes_month_two = 0
    sum_writes_month_three = 0
    day = 0

    writes = write_metrics["Datapoints"]
    for metric in writes:
        sum_writes += metric["Sum"]
        write_list.append(metric["Sum"])

        if day < 30:
            sum_writes_month_one += metric["Sum"]
        elif day < 60:
            sum_writes_month_two += metric["Sum"]
        elif day < 90:
            sum_writes_month_three += metric["Sum"]
        day += 1

    logger.debug(f'for three months, total consumed write units is {sum_writes}')

    read_metrics = get_dynamo_table_metric(
        table,
        ["Sum"],
        "ConsumedReadCapacityUnits",
        periods["daily"],
        "Count",
        three_months_ago,
        yesterday
    )
    sum_reads = 0
    sum_reads_month_one = 0
    sum_reads_month_two = 0
    sum_reads_month_three = 0

    reads = read_metrics["Datapoints"]
    read_list = []
    day = 0
    for metric in reads:
        read_list.append(metric["Sum"])
        sum_reads += metric["Sum"]
        if day < 30:
            sum_reads_month_one += metric["Sum"]
        elif day < 60:
            sum_reads_month_two += metric["Sum"]
        elif day < 90:
            sum_reads_month_three += metric["Sum"]
        day += 1

    logger.debug(f'for three months, total consumed read units is {sum_reads}')

    month_one_price_on_demand = calculate_on_demand_price(sum_writes_month_one, sum_reads_month_one)
    month_two_price_on_demand = calculate_on_demand_price(sum_writes_month_two, sum_reads_month_two)
    month_three_price_on_demand = calculate_on_demand_price(sum_writes_month_three, sum_reads_month_three)

    provisioned_price = calculate_provisioned_price(table)

    # 1 unit provisioned = 2,592,000 consumed units. The first check on every table
    # is just to determine if we have crossed that threshold in three months. if not,
    # then on-demand is far better
    if sum_writes < 2592000 and sum_reads < 2592000:
        if pricing_model == "on-demand":
            logger.info(f'{table} on-demand was most efficient (3 month total): writes({sum_writes}) reads({sum_reads})')
        else:
            logger.warning(f'{table} on-demand would have saved money(3 month total): writes({sum_writes}) reads({sum_reads})')
    elif (sum_writes_month_one < 2592000
          and sum_writes_month_two < 2592000
          and sum_writes_month_three < 2592000
          and sum_reads_month_one < 2592000
          and sum_reads_month_two < 2592000
          and sum_reads_month_three < 2592000):
        if pricing_model == "on-demand":
            logger.info(f'{table} on-demand was the best choice (one month totals)')
        else:
            logger.warning(f'Table {table} on-demand would have saved money (one month totals)')
        logger.info(f'    writes({sum_writes}) reads({sum_reads})')

    elif (month_one_price_on_demand < minimum_provisioned_price
          and month_two_price_on_demand < minimum_provisioned_price
          and month_three_price_on_demand < minimum_provisioned_price):
        if pricing_model == "on-demand":
            logger.info(f'{table} on-demand was the best choice (monthly on demand prices below minimum)')
        else:
            logger.warning(f'{table} on-demand would have saved money (monthly on demand prices below minimum)')
        logger.info(f'    writes({sum_writes}) reads({sum_reads})')

    elif (month_one_price_on_demand < provisioned_price
          and month_two_price_on_demand < provisioned_price
          and month_three_price_on_demand < provisioned_price):
        logger.warning(f'{table} should be examined(provisioned price exceeded on-demand price)')
        logger.info(f'    writes({sum_writes}) reads({sum_reads})')
        logger.info(f'    calculated monthly provisioned price: {provisioned_price}')
        logger.info(f'    month_one_price(on-demand): {month_one_price_on_demand}')
        logger.info(f'    month_two_price(on-demand): {month_two_price_on_demand}')
        logger.info(f'    month_three_price(on-demand): {month_three_price_on_demand}')
    else:
        logger.warning(f'For table {table}, No suggestion found. here are the details')
        logger.info(f'    writes({sum_writes}) reads({sum_reads})')
        logger.info(f'    minimum_provisioned_price: {minimum_provisioned_price}')
        logger.info(f'    calculated monthly provisioned price: {provisioned_price}')
        logger.info(f'    month_one_price(on-demand): {month_one_price_on_demand}')
        logger.info(f'    month_two_price(on-demand): {month_two_price_on_demand}')
        logger.info(f'    month_three_price(on-demand): {month_three_price_on_demand}')




