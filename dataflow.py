import os
import logging
from datetime import timedelta, datetime, timezone

import pandas as pd
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from influx_connector import InfluxDBSink, InfluxDBSource

TOKEN = os.getenv("INLFUXDB_TOKEN", "<your InfluxDB token>")
DATABASE = os.getenv("INFLUXDB_DATABASE", "<your InfluxDB Database>")
ORG = os.getenv("INFLUXDB_ORG", "<your org ID>")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Your custom aggregation query
query = """
SELECT
  date_bin(INTERVAL '15 seconds', time, TIMESTAMP '1970-01-01 00:00:00Z') AS time,
  avg("usage_system") AS usage_system_avg
FROM "cpu"
WHERE
  time >= now() - INTERVAL '1 minute'
GROUP BY 1
ORDER BY time DESC
"""

# Dataflow setup for querying and aggregating values
flow = Dataflow("a_simple_query")

# Create InfluxDBSource with the custom query
inp = op.input("inp", flow, InfluxDBSource(
    timedelta(seconds=10),  # Poll every 10 seconds
    "<your host URL i.e. https://us-east-1-1.aws.cloud2.influxdata.com>",
    DATABASE,
    TOKEN,
    "cpu",  # Measurement name
    ORG,
    datetime.now(timezone.utc) - timedelta(minutes=1),  # Query data from the last minute
    query=query  # Pass the custom query
))

# Inspect the input data
op.inspect("input_query", inp)

# Use the custom sink to write the DataFrame directly back to InfluxDB
op.output("out", inp, InfluxDBSink(
    host="https://us-east-1-1.aws.cloud2.influxdata.com",
    database=DATABASE,
    token=TOKEN,
    org=ORG,
    data_frame_measurement_name="cpu_aggregated",
    # data_frame_tag_columns=['cpu'],  # Specify and columns that are tags if applicable
    data_frame_timestamp_column='time'  # Specify the column that contains timestamps
))

