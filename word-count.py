from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

if len(sys.argv) != 2:
    print("Usage: wordcount <file>", file=sys.stderr)
    exit(-1)

spark = SparkSession\
    .builder\
    .appName("PythonWordCount")\
    .getOrCreate()

lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
output = counts.collect()
longest_word_length = [0, '']
longestCount = [0, '']
for (word, count) in output:
    if(longest_word_length[0] < len(word)):
      longest_word_length = [len(word), word]
    if(longestCount[0] < count):
      longestCount = [count, word]
    print("%s: %i" % (word, count))

spark.stop()
print("\n\n==============\n")
print("Longest word: \"%s\"" % (longest_word_length[1]))
print("Most numerous word: \"%s\" %i" % (longestCount[1], longestCount[0]))
print("\n\n==============\n")
