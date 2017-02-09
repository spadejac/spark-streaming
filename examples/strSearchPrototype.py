


def strMatchPositions(sequence, subsequence):
	matchPositions = []
	match = sequence.find(subsequence)
	while match >=0:
		yield match
		match = sequence.find(subsequence, match+1)


seq = 'CCCAAGTAGTAGTACAGTA'
sub = 'AGTA'
print strMatchPositions(seq,sub)


seq = 'AAGTACGTGCAGTGAGTAGTAGACCTGACGTAGACCGATATAAGTAGCTA'
sub = 'AGTA'
print strMatchPositions(seq,sub)

def getSubsequence(data, searchStr, prefixLen, suffixLen):
	subsequences = []
	matchPositions = strMatchPositions(sequence=data, subsequence=searchStr)
	for pos in matchPositions:
		start = max(pos-prefixLen,0)
		end = min(len(data), pos+suffixLen+len(searchStr))
		subsequences.append(data[start:end])
	print subsequences

getSubsequence(data=seq, searchStr=sub, prefixLen=5, suffixLen=7)
getSubsequence(data=seq, searchStr=sub, prefixLen=0, suffixLen=0)
