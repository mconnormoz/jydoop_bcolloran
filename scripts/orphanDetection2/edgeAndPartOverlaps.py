import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/getJaccardWeightsAndInitParts.py ./outData/weightedEdgesInParts /user/bcolloran/outData/recordsSharingDatePrint/part-r*" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
# def skip_local_output():
#     return True


setupjob = jydoop.setupjob


def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)



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

    lowestPart = min(setOfPartsTouchingRecord)


    if len(setOfPartsTouchingRecord)>1:
        #in this case, the parts overlap; we need to pass the LOWER part to the bin of the HIGHER part in the next MR job, so that the edges touching that part can be re-labeled into the lower part.
        for part in setOfPartsTouchingRecord:
            if part!=lowestPart:
                context.write()





