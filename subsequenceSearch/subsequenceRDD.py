#!/usr/bin/env python
'''
Created on Nov 4, 2016
'''

__author__     = 'Manoj Pillay'
__email__      = 'mpillay [at] lbl [dot] gov'
__license__    = 'The MIT License - https://github.com/spadejac/web-python/blob/master/LICENSE.md'
__status__     = 'Development'
__maintainer__ = 'Manoj Pillay'
__version__    = '1.0'
__copyright__  = '2016 Lawrence Berkeley National Laboratory. All rights reserved.'
__updated__    = '2016-11-04'

from argparse import ArgumentParser
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def strMatchPositions(sequence, subsequence):
    match = sequence.find(subsequence)
    while match >=0:
        yield(match)
        match = sequence.find(subsequence, match+1)
    
    
    
def getSubsequence(rdd):
    terminateSignalReceived = False
    exitSymbol = unichr(949)
    dataAsList = rdd.collect()
    searchStr = args.targetSequence
    prefixLen = args.prefixLength
    suffixLen = args.suffixLength
    
    if dataAsList:
        for data in dataAsList:
            if exitSymbol in data:
                terminateSignalReceived = True
                data = data.replace(exitSymbol,'')
            
            matchPositions = strMatchPositions(sequence=data, subsequence=searchStr)
            for pos in matchPositions:
                start = max(pos-prefixLen,0)
                end = min(len(data), pos+suffixLen+len(searchStr))
                print data[start:end]
        
    if terminateSignalReceived:
        print "Ending marker found. Stopping"
        ssc.stop()
        
# Create a local StreamingContext with two working threads and a batch interval of 2 seconds
if __name__ == '__main__':
    parser = ArgumentParser(description='Subsequences in context. Accepts standard input via local netcat server on port 3333')
    parser.add_argument('-T', '--targetSequence', required=True, dest='targetSequence',
                        help='Nucleotide subsequence to find within standard input')
    parser.add_argument('-x', '--prefixLength', dest='prefixLength', type=int, default=0,
                        help='The number of preceeding characters to display in context')
    parser.add_argument('-y', '--suffixLength', dest='suffixLength', type=int, default=0,
                        help='The number of succeeding characters to display in context')
     
    args = parser.parse_args()

    sc = SparkContext(master="local[2]", appName="StreamingSubsequence")
    ssc = StreamingContext(sc,5.0)
    # Create a Dstream
    nucleotides = ssc.socketTextStream(hostname="localhost", port=3333)    
    nucleotides.foreachRDD(getSubsequence)
    ssc.start()
    ssc.awaitTermination()
