#!/usr/bin/env python
'''
Created on Nov 6, 2016
'''

__author__     = 'Manoj Pillay'
__email__      = 'mpillay [at] lbl [dot] gov'
__license__    = 'The MIT License - https://github.com/spadejac/web-python/blob/master/LICENSE.md'
__status__     = 'Development'
__maintainer__ = 'Manoj Pillay'
__version__    = '1.0'
__copyright__  = '2016 Lawrence Berkeley National Laboratory. All rights reserved.'
__updated__    = '2016-11-06'


from argparse import ArgumentParser
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import sys
import functools

class NucleotideStream(object):
    
    def __init__(self, host, port, targetSeq, prefixLen, suffixLen):
        
        reload(sys)
        sys.setdefaultencoding("utf-8")
        self.exitSymbol = unichr(949)
        self.outFile = 'sparkOutput.txt'
        
        self.host      = host
        self.port      = port
        self.targetSeq = targetSeq
        self.prefixLen = prefixLen
        self.suffixLen = suffixLen
        
        
        self.sc = SparkContext("local[2]", appName="streamingSubsequenceSearch")
        self.ssc = StreamingContext(self.sc, 5.0)
        
    def __enter__(self):
        self.ofh = open(self.outFile)
        return self
    
    def __exit__(self, Type, Value, Traceback):
        self.ofh.close()
        self.ssc.stop()
    
    @staticmethod
    def getMatchPositions(sequence, subsequence):
        match = sequence.find(subsequence)
        while match >=0:
            yield(match)
            match = sequence.find(subsequence, match+1)
        
    
    def getSubsequence(self, line):
        subsequences = []
        if self.exitSymbol in line:
            line = line.replace(self.exitSymbol, '')
        
        matchPositions = self.getMatchPositions(sequence=line, subsequence=self.targetSeq)
        for pos in matchPositions:
            start = max(pos-self.prefixLen, 0)
            end = min( len(line), pos+self.suffixLen+len(self.targetSeq) )
            subsequences.append(line[start:end])
        
        return subsequences
    
    
        
    
    def search(self, nucStream):
        subseqList = ( nucStream.flatMap(lambda l: self.getSubsequence(l))
                                .filter(lambda seqList:bool(seqList))
                     )
        subsequences = subseqList.collect()
        if subsequences:
            print '\n'.join(subsequences)
            self.ofh.write('\n'.join(subsequences))
            self.ofh.write('\n')
        
        exitSignal = ( nucStream.map( lambda l: True if self.exitSymbol in l else False)
                                .fold(False, lambda bool_a,bool_b: bool_a or bool_b)
                     )
        
        if exitSignal:
            print "End of marker encountered"
            self.ssc.stop()
        
        
    def serve(self):

        nucStream = self.ssc.socketTextStream(hostname=self.host, port=self.port)
        search = functools.partial(self.search, self)
        nucStream.foreachRDD(search)
        self.ssc.start()
        self.ssc.awaitTermination()
        
if __name__ == '__main__':
    parser = ArgumentParser(description='Subsequences in context. Accepts standard input via local netcat server on port 3333')
    parser.add_argument('-T', '--targetSequence', required=True, dest='targetSequence',
                        help='Nucleotide subsequence to find within standard input')
    parser.add_argument('-x', '--prefixLength', dest='prefixLength', type=int, default=0,
                        help='The number of preceeding characters to display in context')
    parser.add_argument('-y', '--suffixLength', dest='suffixLength', type=int, default=0,
                        help='The number of succeeding characters to display in context')
    parser.add_argument('-H', '--hostName', dest='hostName', type=str, default='localhost',
                        help='Hostname to read streaming from')
    parser.add_argument('-p', '--port', dest='port', type=int, default=3333,
                        help='Port to read streaming from')
    
    args = parser.parse_args()
    
    with NucleotideStream(host=args.hostName, port=args.port, targetSeq=args.targetSequence, 
                          prefixLen=args.prefixLength, suffixLen = args.suffixLength) as ns:
        ns.serve()
    

            
        
