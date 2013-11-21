import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/docIdsInParts.py ./outData/finalDocIdsInParts ./outData/partsOverlap" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
def skip_local_output():
    return True


setupjob = jydoop.setupjob


def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)

def localTextInput(mapper):
    #local feeds a line of text input to the function after cleaning it up
    #just ignore the line key. split
    if jydoop.isJython():
        return mapper
    else:
        def localMapper(lineKey,inputLine,context):
            keyValList = inputLine.split("\t")
            return mapper(eval(keyValList[0]),eval(keyValList[1]),context)
        return localMapper

class edgeTupError(Exception):
    def __init__(self, part, tupleOfEdges, tupleOfEdgesIter):
        self.part = part
        self.tupleOfEdges = tupleOfEdges
        self.tupleOfEdgesIter =tupleOfEdgesIter
    def __str__(self):
        return repr((self.part, self.tupleOfEdges,self.tupleOfEdgesIter))

'''
input key will be a PART

input val will be either:
1) a LIST of several weightedRecordEdges (a tuple of edges, actually)
2) another PART

where: 
    weightedRecordEdge = (docId_i, docId_j, weight_ij)
    part = ("PART",partNum)

'''
@localTextInput
def map(part,tupleOfEdges,context):
    #recordEdge[0] and recordEdge[1] are the docIds of the two records connected by this edge
    context.write(part,tupleOfEdges)



def reduce(part, tupleOfEdgesIter, context):

    setOfDocIds = set()

    #go through iterOfVals sorting PARTS from edges
    for tupleOfEdges in tupleOfEdgesIter:
        try:
            setOfDocIds.add(tupleOfEdges[0])
            setOfDocIds.add(tupleOfEdges[1])
        except:
            raise edgeTupError(part,tupleOfEdges,list[tupleOfEdgesIter])
        

    context.write(part,tuple(setOfDocIds))






