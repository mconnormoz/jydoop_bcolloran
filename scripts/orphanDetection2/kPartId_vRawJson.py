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
def map(docId,partOrJson,context):
    context.write(docId,partOrJson)



def reduce(docId, iterOfPartOrJson, context):
    jsonsForThisPart=[]
    for partOrJson in iterOfPartOrJson:
        if type(partOrJson)==tuple: #the part for these docs
            partId = partOrJson[1]
        else: #a record in the part
            jsonsForThisPart.append(partOrJson)

    missedRecords = 0

    for json in jsonsForThisPart:
        try:
            context.write(partId,json)
        except:
            missedRecords+=1
            print docId,missedRecords






