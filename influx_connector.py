from typing import Union, Optional
from datetime import datetime, timedelta, timezone
import pandas as pd

from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3.write_client.client.write.point import DEFAULT_WRITE_PRECISION


class _InfluxDBPartition(StatefulSourcePartition):
    def __init__(self, interval, now, last_time, host, database, token, measurement, org, query):
        self.interval = interval
        self.last_time = last_time
        self._next_awake = now
        self.client = InfluxDBClient3(host=host,
                                      database=database,
                                      token=token,
                                      org=org)
        self.host = host
        self.database = database
        self.token = token
        self.measurement = measurement
        self.org = org
        self.query = query  # Store the custom query

    def next_batch(self):
        current_time = datetime.now(timezone.utc)
        # Execute the provided query and return the result as a DataFrame
        data = self.client.query(query=self.query, mode="pandas")
        self.last_time = current_time
        self._next_awake = current_time + self.interval
        return [data] 

    def snapshot(self):
        return {
            "last_time": self.last_time
        }

    def next_awake(self):
        return self._next_awake


class InfluxDBSource(FixedPartitionedSource):
    def __init__(self, 
                 interval: timedelta,
                 host: str,
                 database: str,
                 token: str,
                 measurement: str,
                 org: Optional[str] = None,
                 start_time: Union[datetime, None] = None,
                 query: str = None):
        self.interval = interval
        self.host = host
        self.database = database
        self.token = token
        self.measurement = measurement
        self.org = org
        self.start_time = start_time
        self.query = query

    def list_parts(self):
        return ["singleton"]

    def build_part(self, step_id, for_part, resume_state):
        assert for_part == "singleton"
        resume_state = resume_state or {}
        now = datetime.now(timezone.utc)
        last_time = resume_state.get("last_time", self.start_time)
        if not last_time:
            last_time = now - self.interval
        return _InfluxDBPartition(self.interval, now, last_time, self.host, self.database, self.token, self.measurement, self.org, self.query)


class InfluxDBSinkPartition(StatelessSinkPartition):
    def __init__(self, 
                 host: str, 
                 database: str, 
                 token: str, 
                 org: str,
                 write_precision: str,
                 measurement_name: str = "cpu_aggregated",
                 **kwargs) -> None:
        self.client = InfluxDBClient3(
            host=host,
            database=database,
            token=token,
            org=org
        )
        self.bucket = database
        self.org = org
        self.write_precision = write_precision
        self.measurement_name = measurement_name  # Default measurement name
        self.kwargs = kwargs

    def write_batch(self, items):
        if isinstance(items, pd.DataFrame):
            # Write the DataFrame to InfluxDB
            self.client._write_api.write(
                bucket=self.bucket,
                record=items,
                data_frame_measurement_name=self.measurement_name,
                data_frame_tag_columns=self.tag_columns,
                data_frame_timestamp_column=self.timestamp_column,
                write_precision=self.write_precision,
                **self.kwargs
            )
        else:
            # Handle other types as in the original code
            self.client.write(items, self.bucket, write_precision=self.write_precision, **self.kwargs)

    def close(self):
        self.client.close()


# Custom Sink class that wraps the SinkPartition
class InfluxDBSink(DynamicSink):
    def __init__(self,
                 host: str, 
                 database: str, 
                 token: str, 
                 org: str,
                 write_precision: str = DEFAULT_WRITE_PRECISION,
                 **kwargs) -> None:
        self.host = host
        self.database = database
        self.token = token
        self.org = org
        self.write_precision = write_precision
        self.kwargs = kwargs
            
    def build(self, step_id: str, worker_index: int, worker_count: int
              ) -> InfluxDBSinkPartition:
        return InfluxDBSinkPartition(
            self.host,
            self.database,
            self.token,
            self.org,
            self.write_precision,
            **self.kwargs
        )
