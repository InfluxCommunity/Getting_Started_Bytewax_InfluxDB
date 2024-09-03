# Getting_Started_Bytewax_InfluxDB
An example of how to perform downsampling with Bytewax and InfluxDB Cloud v3. Bytewax is an open source real-time stream processing tool for buidlign data pipelines through a Python framewor. 

## Requirements 
- Python 3.8 or higher
- Latest version of Bytewax
- Latest version of Pandas
- InfluxDB v3 Client

## Environment Variables
Make sure to set the following environment variables for InfluxDB (or hardcode them in the scripts):
```bash
INLFUXDB_TOKEN: Your InfluxDB authentication token.
INFLUXDB_DATABASE: The name of your InfluxDB database.
INFLUXDB_ORG: Your InfluxDB organization name.
```
You can set them in your shell session like this:

```bash
export INLFUXDB_TOKEN="your-token-here"
export INFLUXDB_DATABASE="cpu"
export INFLUXDB_ORG="your-org-here"
```

## Installation and Run

First, clone the repository and navigate to the project directory:

```bash
git clone https://github.com/InfluxCommunity/Getting_Started_Bytewax_InfluxDB
cd Getting_Started_Bytewax_InfluxDB
```

Next set up a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
```
## basic_request.py
This script sets up a dataflow using Bytewax to periodically query data from an InfluxDB database every 15 seconds, specifically retrieving the last 15 seconds of data from the cpu measurement. The retrieved data is processed and then output to the standard output (console) using the StdOutSink. Logging is configured to display information messages, and the flow is set up to log the output as it processes the data. 

Run basic_request.py with:
```bash 
python3 -m bytewax.run basic_request.py
```


## dataflow.py
This script provides an example that uses custom sink and sources defined in [influx_connetor.py](./influx_connector.py) to downsample 1 minute of data every 10 seconds by:
1. Querying InfluxBD and returing a dataframe.
2. Using SQL to aggregate the values.
3. Writing the downsampled dataframe back to InfluxDB.
   
Run dataflow.py with:
```bash 
python3 -m bytewax.run dataflow.py 
```
