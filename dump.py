#!env python3
import numpy as np

import pandas as pd
import sys

df = pd.read_parquet(sys.argv[1])

# Kann man gar nicht so viel fressen wie man kotzen möchte
str_df = df.select_dtypes([np.object])
if len(str_df.dtypes) > 0:
	str_df = str_df.stack().str.decode('utf-8').unstack()
	for col in str_df:
	    df[col] = str_df[col]

df.to_csv(sys.stdout, sep='\t', header=False, index=False, encoding='utf8')
