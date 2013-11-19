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
    def localMapper(lineKey,inputLine,context):
        keyValList = inputLine.split("\t")
        return mapper(eval(keyValList[0]),eval(keyValList[1]),context)
    return localMapper

'''
input KEY will be a PART

input VAL will be either:
1) a LIST of several weightedRecordEdges (a tuple of edges, actually)
2) another PART

where: 
    weightedRecordEdge = (docId_i, docId_j, weight_ij)
    part = ("PART",partNum)

'''
@localTextInput
def map(part,val,context):
    #recordEdge[0] and recordEdge[1] are the docIds of the two records connected by this edge
    context.write(part,val)



def reduce(part, iterOfVals, context):

    setOfEdges = set()

    setOfParts = set()
    #initialize the set of parts under consideration with the key part
    setOfParts.add(part)
    # context.getCounter("GRAPH_STATS", "NUM_INPUT_PARTS").increment(1)

    #go through iterOfVals sorting PARTS from edges
    for val in iterOfVals:
        if val[0]=="PART":
            setOfParts.add(val)
            # context.getCounter("GRAPH_STATS", "part added to set").increment(1)
        else:
            setOfEdges = setOfEdges.union(val)
            # context.getCounter("GRAPH_STATS", "sets of edges union").increment(1)

    lowestPart = min(setOfParts, key = lambda part:part[1])

    for edge in setOfEdges:
        context.write(edge,part)
        # context.getCounter("GRAPH_STATS", "(edge,part) emitted").increment(1)






