#!env python3
import pandas as pd
import sys

pd.read_parquet(sys.argv[1]).to_csv(sys.stdout, sep='\t', header=False, index=False)
