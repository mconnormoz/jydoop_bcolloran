import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/getJaccardWeightsAndInitParts.py ./outData/weightedEdgesInParts /user/bcolloran/outData/recordsSharingDatePrint" hadoop

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


'''
input key will be a PART

input val will be either:
1) a list of weightedRecordEdges
2) another PART

where: 
    weightedRecordEdge = (docId_i, docId_j, weight_ij)
    part = ("PART",partNum)

'''
def map(part,val,context):
    #recordEdge[0] and recordEdge[1] are the docIds of the two records connected by this edge
    context.write(part,val)



def reduce(part, iterOfVals, context):

    setOfEdges = set()

    setOfParts = set()
    #initialize the set of parts under consideration with the key part
    setOfParts.add(part)

    #go through iterOfVals sorting PARTS from edges
    for val in iterOfEdgesAndParts:
        if val[0]=="PART":
            setOfParts.add(val)
        else:
            setOfEdges.add(val)

    lowestPart = min(setOfParts, key = lambda part:part[1])

    for edge






