import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/docIdsInParts.py ./outData/finalDocIdsInParts ./outData/partsOverlap" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
# def skip_local_output():
    # return True


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
    def __init__(self, part, listOfEdges, iterOfEdges):
        self.part = part
        self.listOfEdges = listOfEdges
        self.iterOfEdges =iterOfEdges
    def __str__(self):
        return repr((self.part, self.listOfEdges,self.iterOfEdges))


def counterLocal(context,counterGroup,countername,value):
    if jydoop.isJython():
        context.getCounter(counterGroup, countername).increment(value)
    else:
        pass




'''
input key will be a PART

input val will be a LIST of several weightedRecordEdges (a tuple of edges, actually)

where: 
    part = ("PART",partNum)
    weightedRecordEdge = (docId_i, docId_j, weight_ij)

'''
@localTextInput
def map(part,listOfEdges,context):
    #recordEdge[0] and recordEdge[1] are the docIds of the two records connected by this edge
    counterLocal(context,"GRAPH_STATS", "edges into mapper",1)

    for edge in listOfEdges:
        context.write((edge[0],part),1)
        context.write((edge[1],part),1)



def reduce(docId_partId, junkVal, context):
    context.write(docId_partId[0],docId_partId[1])
    counterLocal(context,"GRAPH_STATS", "records out of reducer",1)






