#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    	# remove leading and trailing whitespace
	line = line.strip()
	# split the line into words
	words = line.split()
	# increase counters
	user = words[0]
	
	# write the results to STDOUT (standard output);
	# what we output here will be the input for the
	# Reduce step, i.e. the input for fof.reducer.py
	end_idx = len(words)
	for i in xrange(1,end_idx):
		for j in xrange(i+1,end_idx):
			cand = [user, words[i], words[j]]
			cand.sort()
			print '%s,%s,%s\t%s' % (cand[0], cand[1], cand[2], 1)
	

