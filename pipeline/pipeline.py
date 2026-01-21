import sys
import pandas as pd

print(sys.argv)
month = sys.argv[1]

print(f'hello world, month={month}')


df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

