#!/usr/bin/env python
'''
Created on Nov 6, 2016
'''

__author__     = 'Manoj Pillay'
__email__      = 'mpillay [at] lbl [dot] gov'
__license__    = 'The MIT License'
__status__     = 'Development'
__maintainer__ = 'Manoj Pillay'
__version__    = '1.0'
__copyright__  = '2016 Lawrence Berkeley National Laboratory. All rights reserved.'
__updated__    = '2016-11-06'

from argparse import ArgumentParser
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

def strMatchPositions(sequence, subsequence):
    '''
    Walks through a string(sequence) to find occurrences of another string(subsequence) and yields position indexes 
    
    @param sequence    : <str> reference string that will be searched
    @param subsequence : <str> target search string
    @return : 
    '''
    match = sequence.find(subsequence)
    while match >=0:
        yield(match)
        match = sequence.find(subsequence, match+1)
    
    
def getSubsequence(line, searchStr, prefixLen, suffixLen):
    '''
    Accepts reference and target sequences, window sizes to generate list of subsequences
    
    @param line      : <unicode> text in unicode that will be searched against
    @param searchStr : <str> string to search for - target sequence
    @param prefixLen : <int> prefix length --> number of characters to precede the matched target sequence
    @param suffixLen : <int> suffix Length --> number of characters to succeed the matched target sequence
    @return: <list> list of subsequences meeting search conditions     
    '''
    subsequences = []
    if exitSymbol in line:
        line = line.replace(exitSymbol, '')
    
    matchPositions = strMatchPositions(sequence=line, subsequence=searchStr)
    for pos in matchPositions:
        start = max(pos-prefixLen,0)
        end = min(len(line), pos+suffixLen+len(searchStr))
        subsequences.append(line[start:end])
    return subsequences
    
def search(nucRDD):
    '''
    Accepts an RDD as input, delegates subsequence search and prints output.
    Closes streaming context when exit character is encountered.
    @return: None
    '''
    subseqList = (        # Perform subsequence search for each item within RDD  
                   nucRDD.flatMap(lambda l: getSubsequence(l, searchStr=args.targetSequence, 
                                                              prefixLen=args.prefixLength, suffixLen=args.suffixLength))
                          # Filter results from non matching and/or empty streams 
                         .filter(lambda seqList:bool(seqList))
                 )
    # Convert results from a single RDD to a list
    subsequences = subseqList.collect()
    if subsequences:
        print '\n'.join(subsequences)   # Print newline-separated string output
        # If an output file was added as a command-line argument, write output to it along with STDOUT
        if outputFile:
            outputFile.write('\n'.join(subsequences))
            outputFile.write('\n')
        
    # For conditional termination, poll for the exit character within the stream 
    exitSignal = (nucRDD.map(lambda l:True if exitSymbol in l else False) # Return true if exit symbol encountered, else return False
                        .fold(False, lambda a,b:a or b)                   # Reduce all boolean returns using an OR operation
                 )
    # If the exit signal was encountered within any of the RDDs processed
    if exitSignal:
        print "------------------------------------"
        print "---- Program output ends here ------"
        print "------------------------------------"

        
        print "\nEnd of marker encountered. Now stopping contexts. Hope you had fun playing with this."
        if outputFile:
            print "Results are available for inspection in %s" %outputFile.name
            outputFile.close()      # Close open output file write handle if exists
        ssc.stop()                  # Stop streaming context

if __name__ == '__main__':
    # Manage encoding for unicode exit character epsilon --> unichr(949)
    reload(sys)
    sys.setdefaultencoding('utf-8')
    exitSymbol = unichr(949)

    # Command Line Options
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
    parser.add_argument('-o', '--outputFile', dest='outputFile',
                        help="Filename to write output to")

    
    # Parse command line input
    args = parser.parse_args()
    
    print ('\n Please ensure that a netcat server is running on %s:%s where an STDIN stream can be accepted.' 
           ' This window is for STDOUT.\n' %(args.hostName, args.port))
    
    print "------------------------------------"
    print "---- Program output begins here ----"
    print "------------------------------------"
    # optional output file
    outputFile = open(args.outputFile, 'w') if args.outputFile else None
    
    # Create a local streaming context with 1 second batch intervals
    sc = SparkContext(appName="StreamingSubsequence")
    ssc = StreamingContext(sc,1.0)
    # Create a DStream that will connect to hostname:port, default--> localhost:3333
    nucleotides = ssc.socketTextStream(hostname=args.hostName, port=args.port)    
    
    # Search for subsequences within each RDD generated from the stream
    nucleotides.foreachRDD(search)
    
    ssc.start()                 # Start computation
    ssc.awaitTermination()      # Wait for computation to terminate
    
    if outputFile:
        outputFile.close()