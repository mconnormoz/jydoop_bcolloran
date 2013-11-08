import json
import jydoop

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/getJaccardWeightsAndInitParts.py ./outData/weightedEdgesInParts /user/bcolloran/outData/recordsSharingDatePrint/part-r*" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
# def skip_local_output():
#     return True


setupjob = jydoop.setupjob

# def setupjob(job, args):
#     """
#     Set up a job to run on one or more HDFS locations

#     Jobs expect one or more arguments, the HDFS path(s) to the data.
#     """
#     import org.apache.hadoop.mapreduce.lib.input.FileInputFormat as FileInputFormat
#     import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat as MyInputFormat

#     if len(args) < 1:
#         raise Exception("Usage: <hdfs-location1> [ <location2> ] [ <location3> ] [ ... ]")

#     job.setInputFormatClass(MyInputFormat)
#     FileInputFormat.setInputPaths(job, ",".join(args));
#     job.getConfiguration().set("org.mozilla.jydoop.mappertype", "JYDOOP")
#     # set the job to run in the RESEARCH queue
#     job.getConfiguration().set("mapred.job.queue.name","research")






'''
identity mapper is used. input key vals are of the form
((recordInfo[i],recordInfo[j]), datePrint)
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

    # recordEdge=json.loads(recordEdge)
    try:
        intersection = float(len( sum(1 for _ in set(datePrintIter) ) ))
    except:
        context.write("no_datePrintIter",1)
        intersection = 1

    try:
        union = float(len(set(recordEdge[0][1]).union(recordEdge[1][1])))
    except:
        context.write("no_union",1)
        union = 1


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



