#!/usr/bin/env python3

import json
import sys


def main(args=None):
	jsonin = sys.stdin
	tabout = sys.stdout

	content = jsonin.read()
	data = json.loads(content)
	if not data:
		return

	header = data[0]
	col_lengths = list(map(len, map(str, header)))
	for row in data[1:]:
		for col_ix, val_len in enumerate(map(len, map(str, row))):
			if val_len > col_lengths[col_ix]:
				col_lengths[col_ix] = val_len
	
	header_formats = []
	row_formats = []
	for col_length in col_lengths:
		col_length_str = str(col_length)
		header_formats.append('{!s:^' + col_length_str + '}')
		row_formats.append('{!s:>' + col_length_str + '}')
	header_format = ' | '.join(header_formats)
	row_format = ' | '.join(row_formats)

	print(header_format.format(*data[0]), file=tabout)
	print('-+-'.join(['-' * col_length for col_length in col_lengths]), file=tabout)
	for row in data[1:]:
		print(row_format.format(*row), file=tabout)



if __name__ == '__main__':
	main()
