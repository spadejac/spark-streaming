#!/usr/bin/env python
'''
Created on Nov 2, 2016
'''


__author__     = 'Manoj Pillay'
__email__      = 'mpillay [at] lbl [dot] gov'
__license__    = 'The MIT License - https://github.com/spadejac/web-python/blob/master/LICENSE.md'
__status__     = 'Development'
__maintainer__ = 'Manoj Pillay'
__version__    = '1.0'
__copyright__  = '2016 Lawrence Berkeley National Laboratory. All rights reserved.'
__updated__    = '2016-11-02'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working threads and a batch interval of 2 seconds
sc = SparkContext(master="local[2]", appName="NetworkWordCount")
ssc = StreamingContext(sc,5.0)

# Create a Dstream
lines = ssc.socketTextStream(hostname="localhost", port=3333)

# Split each line into words
wordCounts = ( lines.flatMap(lambda line: line.split(" "))  # Split each line into words
                  .map(lambda word: (word, 1)) 
                  .reduceByKey(lambda x,y: x+y)             # Count each word in each batch
             )


#pairs = words.map(lambda word: (word, 1))
#wordCounts = pairs.reduceByKey(lambda x,y: x+y)

#Print each batch
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()