import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/relabelEdges.py ./outData/relabeledEdges ./outData/partsOverlap" hadoop

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

    setOfEdges = set()

    #go through iterOfVals sorting PARTS from edges
    for tupleOfEdges in tupleOfEdgesIter:
        setOfEdges = setOfEdges.union(tupleOfEdges)

    context.write(part,tuple(setOfEdges))






