import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/getJaccardWeightsAndInitParts.py ./outData/weightedEdgesInParts /user/bcolloran/outData/recordsSharingDatePrint" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
def skip_local_output():
    return True


setupjob = jydoop.setupjob




'''
identity mapper is used.
input key;vals are of the form
(recordInfo[i],recordInfo[j]); datePrint
and we need these immediately sorted+binned into reducers.

each "recordInfo" is of the form
(fhrDocId,datePrints)

'''


def map(recordEdge,datePrint,context):
    context.write(recordEdge,datePrint)


def jaccard(a, b):
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))



def reduce(recordEdge, datePrintIter, context):
    # recordEdge is (recordInfo_i,recordInfo_j)
    # recordInfo_i is (fhrDocId,datePrintList)
    # recordEdge[0][1] is the datePrints in record_i
    # recordEdge[1][1] is the datePrints in record_j
    # datePrintIter contains the intersection of days in both records

    printsCommonToEdgeEnds = set()
    for d in datePrintIter:
        printsCommonToEdgeEnds.add(d)

    intersection = float(len(printsCommonToEdgeEnds))

    union = float(len(set(recordEdge[0][1]).union(recordEdge[1][1])))

    try:
        # note that after this mapred pass, the datePrintList for each record is no longer needed, so we can pass out a new recordEdge with just the docIds
        weightedRecordEdge = tuple(sorted([recordEdge[0][0],recordEdge[1][0]])+[intersection/union])
        context.write(weightedRecordEdge,("PART",min([recordEdge[0][0],recordEdge[1][0]])))

    except:
        try:
            context.write("recordEdge[0]",str(recordEdge[0]))
        except:
            context.write("NO_recordEdge[0]",str(recordEdge[0]))
        try:
            context.write("recordEdge[1]",str(recordEdge[1]))
        except:
            context.write("NO_recordEdge[1]",str(recordEdge[1]))






def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)



