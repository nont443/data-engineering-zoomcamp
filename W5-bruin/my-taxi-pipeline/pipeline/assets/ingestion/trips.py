"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace
image: python:3.11

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


import os
import json
import pandas as pd
import datetime as dt


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    
    print(start_date)
    print(end_date)
    print(taxi_types)

    year_start = start_date[0:4]
    month_start = start_date[5:7]
    print(year_start)
    print(month_start)

    year_end = end_date[0:4]
    month_end = end_date[5:7]
    print(year_end)
    print(month_end)

    taxi_types='yellow'

    url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_types}_tripdata_{year_start}-{month_start}.parquet"
    print(url)
    df = pd.read_parquet(url,engine="pyarrow")
    day = dt.date.today()
    print(day)
    df["load_dt"] = day

    # df = df[:200]
    df = df.rename(columns={"tpep_pickup_datetime":"pickup_datetime",
                       "tpep_dropoff_datetime":"dropoff_datetime"})
    df.head()

    # df = pd.DataFrame({"date":[1,2,3],"month":[4,5,6]})

    # Generate list of months between start and end dates
    # Fetch parquet files from:
    # https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet

#    return final_dataframe
    return df
