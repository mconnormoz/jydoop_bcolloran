import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/edgeAndPartOverlaps.py ./outData/weightedEdgesInParts /user/bcolloran/outData/recordsSharingDatePrint" hadoop

---to iterate
make ARGS="scripts/orphanDetection2/edgeAndPartOverlaps.py ./outData/partsOverlap2 /user/bcolloran/outData/relabeledEdges" hadoop


-- dump an iteration to text
make ARGS="scripts/dumpHdfsKeyValsToStrings.py ./outData/partsOverlap7 ./outData/partsOverlap7" hadoop
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
input key; val --
weightedRecordEdge; part

where: 
    weightedRecordEdge = (docId_i, docId_j, weight_ij)
    part = ("PART",partNum)
'''

@localTextInput
def map(recordEdge,part,context):
    #recordEdge[0] and recordEdge[1] are the docIds of the two records connected by this edge
    context.write(recordEdge[0],(recordEdge,part))
    context.write(recordEdge[1],(recordEdge,part))



def reduce(docId, iterOfEdgesAndParts, context):
    setOfEdgesTouchingRecord = set()
    setOfPartsTouchingRecord = set()
    for item in iterOfEdgesAndParts:
        setOfEdgesTouchingRecord.add(item[0])
        setOfPartsTouchingRecord.add(item[1])

    lowestPart = min(setOfPartsTouchingRecord, key = lambda part:part[1])

    #emit the lowest part with a tuple of all the edges it touches
    context.write(lowestPart,tuple(setOfEdgesTouchingRecord))
    context.getCounter("GRAPH_STATS", "NUMBER_OF_RECORDS").increment(1)
    context.getCounter("GRAPH_STATS", "OVERLAPPING_PARTS_THIS_ITER").increment(0)

    if len(setOfPartsTouchingRecord)>1:
        #in this case, the parts overlap; we need to pass the LOWER part to the bin of the HIGHER parts in the next MR job, so that the edges touching that part can be re-labeled into the lower part.
        context.getCounter("GRAPH_STATS", "OVERLAPPING_PARTS_THIS_ITER").increment(1)
        for part in setOfPartsTouchingRecord:
            if part!=lowestPart:
                context.write(part,lowestPart)



