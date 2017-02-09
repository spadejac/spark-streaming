#Table of contents

[TOC]

Description
=========
This document describes the usage details of the attached program **subsequences.py**. It implements an online algorithm in Python using Spark Streaming to find sequences of values and to print them along with surrounding context.

##Prerequisites
To get this program to run correctly, you need the following:

* Python 2.7 
* Spark  1.6.1
	* *PySpark* via **spark-submit**
* Additional Python Modules:
	*  argparse (for parsing command-line arguments)
* A data stream generator such as ***netcat***.

## Running 

To run with default configuration, start a local netcat server on localhost:3333 using the following command:
```
bash:~$ nc -lk 3333
```
This is where you will provide standard input to the program.

To run the program in its simplest form:
```
bash:~$ spark-submit subsequences.py -T <target-sequence>
```

To test the example in the question, you may want to do something like:
```
bash:~$ spark-submit subsequences.py -T AGTA -x 5 -y 7
```
You will see the following welcome message:

     Please ensure that a netcat server is running on localhost:3333 where an STDIN stream can be accepted. This window is for STDOUT.
    
    ------------------------------------
    ---- Program output begins here ----
    ------------------------------------


Following this, please provide standard input using the netcat server running in a separate window. 

Alternatively, you may pipe larger input streams such as files into this program as follows: 
```
 bash:~$ cat <file-name> | nc -lk 3333
```

## Example
**STDIN WINDOW**:
```
pillay@manoj-laptop:~$ nc -lk 3333
AAGTACGTGCAGTGAGTAGTAGACCTGACGTAGACCGATATAAGTAGCTAε
```
**STDOUT WINDOW**:
```
pillay@manoj-laptop:~/workspace/pySandbox/src/spark/streaming$ spark-submit --master local[*] subsequences.py -T AGTA -x 5 -y 7 -o sparkOut.txt

 Please ensure that a netcat server is running on localhost:3333 where an STDIN stream can be accepted. This window is for STDOUT.

------------------------------------
---- Program output begins here ----
------------------------------------
AAGTACGTGCAG
CAGTGAGTAGTAGACC
TGAGTAGTAGACCTGA
ATATAAGTAGCTA
------------------------------------
---- Program output ends here ------
------------------------------------

End of marker encountered. Now stopping contexts. Hope you had fun playing with this.
Results are available for inspection in sparkOut.txt
16/11/06 17:34:33 ERROR ReceiverTracker: Deregistered receiver for stream 0: Stopped by driver

```
##Usage
The general usage of the program is as follows:

    usage: subsequences.py [-h] -T TARGETSEQUENCE [-x PREFIXLENGTH]
                           [-y SUFFIXLENGTH] [-H HOSTNAME] [-p PORT]
                           [-o OUTPUTFILE] Subsequences in context. Accepts standard input via local netcat server on port 3333 optional arguments:   -h, --help            show this help message and exit  
    -T TARGETSEQUENCE, --targetSequence TARGETSEQUENCE
                            Nucleotide subsequence to find within standard input   
    -x PREFIXLENGTH, --prefixLength PREFIXLENGTH
                            The number of preceeding characters to display in context   
    -y SUFFIXLENGTH, --suffixLength SUFFIXLENGTH
                            The number of succeeding characters to display in context   
    -H HOSTNAME, --hostName HOSTNAME
                            Hostname to read streaming from   
    -p PORT, --port PORT  Port to read streaming from   
    -o OUTPUTFILE, --outputFile OUTPUTFILE
                            Filename to write output to



##Author
 Manoj Pillay - ManojPillayM@gmail.com