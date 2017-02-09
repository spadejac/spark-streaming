#!/usr/bin/env python
'''
Created on Nov 5, 2016
'''


__author__     = 'Manoj Pillay'
__email__      = 'mpillay [at] lbl [dot] gov'
__license__    = 'The MIT License - https://github.com/spadejac/web-python/blob/master/LICENSE.md'
__status__     = 'Development'
__maintainer__ = 'Manoj Pillay'
__version__    = '1.0'
__copyright__  = '2016 Lawrence Berkeley National Laboratory. All rights reserved.'
__updated__    = '2016-11-05'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext()
dvc = [[-0.1, -0.1], [0.1, 0.1], [1.1, 1.1], [0.75, 0.75], [0.9, 0.9]]
dvc = [sc.parallelize(i, 1) for i in dvc]
ssc = StreamingContext(sc, 2.0)
input_stream = ssc.queueStream(dvc)

def get_output(rdd):
    rdd_data = rdd.collect()
    if 0.75 in rdd_data:
        print "Ending marker found", rdd_data
        ssc.stop()
    else:
        print "Not found ending marker. Continuing"
        print rdd_data

input_stream.foreachRDD(get_output)
ssc.start()
ssc.awaitTermination()