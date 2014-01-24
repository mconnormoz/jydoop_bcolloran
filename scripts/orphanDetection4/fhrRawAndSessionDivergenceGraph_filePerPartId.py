import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/docIdsInParts.py ./outData/finalDocIdsInParts ./outData/partsOverlap" hadoop

'''


######## to OUTPUT TO HDFS
def skip_local_output():
    return True



setupjob = jydoop.setupjob


def output(path, results):
    # just dump tab separated key/vals
    
    
    partNum=0
    for k, vList in results:
        partNum+=1
        with open(path+"_numRecs"+str(len([v for v in vList if v[0:9]=='{"version']))+("_graph" if [v for v in vList if v[0:7]=='{"nodes'] else "")+"_partNum"+str(partNum), 'w') as f:
            firstLine = True
            for v in vList:
                if firstLine:
                    # print repr(v)
                    f.write(str(v).strip())
                    firstLine=False
                else:
                    f.write("\n"+str(v).strip())


def localTextInput(mapper):
    #local feeds a line of text input to the function after cleaning it up
    #just ignore the line key. split
    if jydoop.isJython():
        return mapper
    else:
        def localMapper(lineKey,inputLine,context):
            keyValList = inputLine.split("\t")
            key = keyValList[0]

            if keyValList[1][0:7]=="('PART'":
                val = eval(keyValList[1])
            else:
                val = keyValList[1]

            return mapper(key,val,context)
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
def map(partId,docOrGraph,context):
    context.write(partId,docOrGraph)

def reduce(partId, iterOfdocOrGraph, context):
    context.write(partId,list(iterOfdocOrGraph))






