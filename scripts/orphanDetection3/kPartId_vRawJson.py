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
    firstLine = True
    with open(path, 'w') as f:
        for k, v in results:
            if firstLine:
                f.write(str(k)+"\t"+str(v))
                firstLine=False
            else:
                f.write("\n"+str(k)+"\t"+str(v))


def localTextInput(mapper):
    #local feeds a line of text input to the function after cleaning it up
    #just ignore the lineKey.
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

    for json in jsonsForThisPart:
        try:
            context.write(partId,json.strip())
        except UnboundLocalError:
            #exception raised if 'partId' not set above
            #this happens id a doc was *not linked to any other records* upstream in the data pipeline.
            context.getCounter("REDUCER", "records without parts").increment(1)






