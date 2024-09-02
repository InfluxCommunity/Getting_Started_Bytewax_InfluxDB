import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from influxdb_client_3 import InfluxDBClient3
from bytewax.inputs import SimplePollingSource
import logging
from datetime import timedelta

# python3 -m bytewax.run basic.py
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# logger.info("Executing query: %s", data)

class InfluxDBSource(SimplePollingSource):
    def next_item(self):
        client = InfluxDBClient3(host=f"<your host URL i.e. us-east-1-1.aws.cloud2.influxdata.com>",
                         database="<your InfluxDB Database>",
                         token=f"<your InfluxDB token>")
        query = "SELECT * from cpu WHERE time >= now() - INTERVAL '15 seconds' LIMIT 5"
        data = client.query(query=query, mode="pandas")
        # data = data["time"][0]
        return (data)

flow = Dataflow("a_simple_example")

stream = op.input("input", flow, InfluxDBSource(timedelta(seconds=15)))
# op.inspect("check_stream", stream)
logger.info("output")
op.output("out", stream, StdOutSink())