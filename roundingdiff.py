#!/usr/bin/env python3

# compare tab-separated result sets, allowing some numerical delta
# 2018-10-24 hannes@cwi.nl

import sys
import pandas
import os
import glob

delta = 0.05

if len(sys.argv) < 3:
	print("Usage: roundingdiff.py file1.ans file2.ans OR roundingdiff.py folder-test folder-reference")
	sys.exit(-1)

def load(fname):
	df = pandas.read_csv(fname, sep='\t', header=None)
	# convert all numbers to floats and strip all strings left/right
	for c in range(len(df.dtypes)):
		if (str(df.dtypes[c]).startswith('int')):
			df[c] = df[c].astype(float)
		if (str(df.dtypes[c]) == 'object'):
			df[c] = df[c].str.strip()
	return df

def diff(f1, f2):
	if not os.path.exists(f1) or not os.path.exists(f2) or os.path.getsize(f1) == 0 or os.path.getsize(f2) == 0:
		return {'class': 'fail', 'group': '', 'message': "No result %s <> %s" % (f1, f2)}

	try:
		p1 = load(f1)
		p2 = load(f2)
	except:
		return {'class': 'fail', 'group': 'loader_exception', 'message': "failed to load file"}


	if len(p1.dtypes) != len(p2.dtypes):
		return {'class': 'diff', 'group': 'diff_column_count', 'message': "Different number of columns: %s:%d, %s:%d" % (f1, len(p1.dtypes), f2, len(p2.dtypes))}


	if not (p1.dtypes == p2.dtypes).all():
		return {'class': 'diff', 'group': 'diff_types', 'message': "Different data types %s <> %s" % (', '.join([str(t) for t in p1.dtypes.tolist()]), ', '.join([str(t) for t in p2.dtypes.tolist()]))}


	if len(p1) != len(p2):
		return {'class': 'diff', 'group': 'diff_row_count', 'message': "Different number of rows: %s:%d, %s:%d" % (f1, len(p1), f2, len(p2))}

	diffs = 0

	for c in range(len(p1.dtypes)):
		v1 = p1[c]
		v2 = p2[c]
		if (str(p1.dtypes[c]) == "object"):
			diffs = diffs + (v1.astype(str) != v2.astype(str)).sum()

		if (str(p1.dtypes[c]) == "float64"):
			diffs = diffs + ((v1-v2).abs() > (v1 * delta).abs()).sum()

	if diffs > 0:
		return {'class': 'diff', 'group': 'diff_data', 'message': "%s != %s, %d diffs" % (f1, f2, diffs)}

	return {'class': 'match', 'group': '', 'message': "%s == %s, %d cols %d rows" % (f1,  f2, len(p1.dtypes), len(p2))}

	return 0

if os.path.isdir(sys.argv[1]) and os.path.isdir(sys.argv[2]):
	tests = glob.glob(os.path.join(sys.argv[1], '*'))
	tests.sort()
	passing = 0
	failing = 0
	for test in tests:
		res = diff(test, os.path.join(sys.argv[2], os.path.basename(test)))
		if res['class'] == 'match':
			passing = passing + 1
		else:
			failing = failing + 1
		print("%s\t%s\t%s" % (os.path.basename(test).replace(".ans", ""), res['class'], res['group']))
else:
	res = diff(sys.argv[1], sys.argv[2])
	print(res)
	sys.exit(int(res["class"] == 'match'));



