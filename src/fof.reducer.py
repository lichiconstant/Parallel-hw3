#!/usr/bin/env python

from operator import itemgetter
import sys

pre_key = ""
pre_key_count = 0
first = True

# input comes from STDIN
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()

	# parse the input we got from mapper.py
	cur_key, count = line.split('\t', 1)

	# convert count (currently a string) to int
	try:
		count = int(count)
	except ValueError:
		# count was not a number, so silently
		# ignore/discard this line
		continue
	
	# this IF-switch only works because Hadoop sorts map output
	# by key (here: word) before it is passed to the reducer	
	if first:
		pre_key = cur_key
		pre_key_count = 1
		first = False
	else:
		if cur_key == pre_key:
			pre_key_count += 1
			if pre_key_count == 2:
				tokens = cur_key.split(',')
				print '<%s,%s,%s>\t%s' % (tokens[0],tokens[1],tokens[2], 1)
				print '<%s,%s,%s>\t%s' % (tokens[1],tokens[0],tokens[2], 1)
				print '<%s,%s,%s>\t%s' % (tokens[2],tokens[0],tokens[1], 1)	
		else:
			pre_key = cur_key
			pre_key_count = 1



