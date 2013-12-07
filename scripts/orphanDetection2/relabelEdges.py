import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/relabelEdges.py ./outData/relabeledEdges ./outData/partsOverlap" hadoop

---to iterate
make ARGS="scripts/orphanDetection2/relabelEdges.py ./outData/orphIterTest/relabeledEdges_2 ./outData/orphIterTest/partsOverlap_1" hadoop

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

def counterLocal(context,counterGroup,countername,value):
    if jydoop.isJython():
        context.getCounter(counterGroup, countername).increment(value)
    else:
        pass

'''
input KEY will be a PART

input VAL will be either:
1) a LIST of several weightedRecordEdges
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
    # counterLocal(context,"GRAPH_STATS", "NUM_INPUT_PARTS",1)
    context.getCounter("REDUCER", "NUM_INPUT_PARTS").increment(1)

    #go through iterOfVals sorting PARTS from edges
    for val in iterOfVals:
        if val[0]=="PART":
            setOfParts.add(val)
            # counterLocal(context,"GRAPH_STATS", "part added to set",1)
            context.getCounter("REDUCER", "PART_COLLISIONS").increment(1)
        else:
            setOfEdges = setOfEdges.union(val)
            # counterLocal(context,"GRAPH_STATS", "sets of edges union",1)
            context.getCounter("REDUCER", "sets of edges union").increment(1)

    lowestPart = min(setOfParts, key = lambda part:part[1])

    for edge in setOfEdges:
        context.write(edge,part)
        context.getCounter("REDUCER", "kEdge_vPart emitted").increment(1)






