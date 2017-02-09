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

sc = SparkContext()

dvc = [[-0.1, -0.1], [0.1, 0.1], [1.1, 1.1], [0.9, 0.9]]
dvc = [sc.parallelize(i, 1) for i in dvc]
ssc = StreamingContext(sc, 2.0)
input_stream = ssc.queueStream(dvc)

def get_output(rdd):
    print(rdd.collect())
input_stream.foreachRDD(get_output)
ssc.start()
ssc.stop()