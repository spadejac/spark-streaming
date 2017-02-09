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
    
    
def getSubsequence(line, searchStr, prefixLen, suffixLen):
    
    subsequences = []
    exitSymbol = unichr(949)
    if exitSymbol in line:
        line = line.replace(exitSymbol, '')
    
    matchPositions = strMatchPositions(sequence=line, subsequence=searchStr)
    for pos in matchPositions:
        start = max(pos-prefixLen,0)
        end = min(len(line), pos+suffixLen+len(searchStr))
        subsequences.append(line[start:end])
    return subsequences
    
def search(nucStream):
    subseqList = ( nucStream.flatMap(lambda l: getSubsequence(l, searchStr=args.targetSequence, 
                                                              prefixLen=args.prefixLength, suffixLen=args.suffixLength))
                            .filter(lambda seqList:bool(seqList))
                 )
    subsequences = subseqList.collect()
    if subsequences:
        print '\n'.join(subsequences)
        outputFile.write('\n'.join(subsequences))
        outputFile.write('\n')
        
    exitSignal = (nucStream.map(lambda l:True if unichr(949) in l else False)
                           .fold(False, lambda a,b:a or b)
                 )
    if exitSignal:
        print "End of marker encountered. Now stopping contexts...."
        outputFile.close()
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
    outputFile = open('sparkOutput.txt', 'w')
    sc = SparkContext(appName="StreamingSubsequence")
    ssc = StreamingContext(sc,5.0)
    # Create a Dstream
    nucleotides = ssc.socketTextStream(hostname="localhost", port=3333)    
    nucleotides.foreachRDD(search)
    ssc.start()
    ssc.awaitTermination()
