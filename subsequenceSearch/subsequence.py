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

def strMatchPositions(sequence, subsequence):
    match = sequence.find(subsequence)
    while match >=0:
        yield(match)
        match = sequence.find(subsequence, match+1)
    
    
    
def getSubsequence(data, searchStr, prefixLen, suffixLen):
    if data.find('exit') >=0:
        print "Ending marker found. Stopping"

        if ssc is not None:
            print "Pretending that it stopped"
            ssc.stop()
    else:
        subsequences = []
        matchPositions = strMatchPositions(sequence=data, subsequence=searchStr)
        for pos in matchPositions:
            start = max(pos-prefixLen,0)
            end = min(len(data), pos+suffixLen+len(searchStr))
            subsequences.append(data[start:end])
    
    
        return subsequences
    
    return None

# Create a local StreamingContext with two working threads and a batch interval of 2 seconds
if __name__ == '__main__':
    subsequences = None
    sc = SparkContext(master="local[2]", appName="StreamingSubsequence")
    ssc = StreamingContext(sc,5.0)
    # Create a Dstream
    nucleotides = ssc.socketTextStream(hostname="localhost", port=3333)    


    parser = ArgumentParser(description='Subsequences in context. Accepts standard input via local netcat server on port 3333')
    parser.add_argument('-T', '--targetSequence', required=True, dest='targetSequence',
                        help='Nucleotide subsequence to find within standard input')
    parser.add_argument('-x', '--prefixLength', dest='prefixLength', type=int, default=0,
                        help='The number of preceeding characters to display in context')
    parser.add_argument('-y', '--suffixLength', dest='suffixLength', type=int, default=0,
                        help='The number of succeeding characters to display in context')
    
    args = parser.parse_args()
        
#     try:
    subsequences = (nucleotides.map(lambda line: getSubsequence(line, searchStr=args.targetSequence, 
                                                                    prefixLen=args.prefixLength, suffixLen=args.suffixLength))
                                   .filter(lambda list: bool(list))
                                   #.map(lambda matches: '\n'.join([match for match in matches]))                           
                       )
        
    subsequences.pprint()
    ssc.start()
    ssc.awaitTermination()
        
#     except UnicodeDecodeError, e:
#         ssc.stop(stopSparkContext=True, stopGraceFully=True)
#     finally:
#         if subsequences:
#             subsequences.pprint()