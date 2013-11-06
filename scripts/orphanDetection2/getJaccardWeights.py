'''
in following commands, UPDATE DATES

----to run against full HBASE, output to HDFS
jydoopRemote peach scripts/getSampleOfFhrPacketsWithDuplicatedFingerprint_2013-11.py outData/v2Packets_sampleWithLinkedOrphans_2013-11-05

'''

######## to OUTPUT TO HDFS from RAW HBASE
def skip_local_output():
    return True


# setupjob = healthreportutils.setupjob

def setupjob(job, args):
    """
    Set up a job to run on one or more HDFS locations

    Jobs expect one or more arguments, the HDFS path(s) to the data.
    """
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat as FileInputFormat
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat as MyInputFormat

    if len(args) < 1:
        raise Exception("Usage: <hdfs-location1> [ <location2> ] [ <location3> ] [ ... ]")

    job.setInputFormatClass(MyInputFormat)
    FileInputFormat.setInputPaths(job, ",".join(args));
    job.getConfiguration().set("org.mozilla.jydoop.mappertype", "TEXT")
    # set the job to run in the RESEARCH queue
    job.getConfiguration().set("mapred.job.queue.name","research")






'''
no mapper is used. input key vals are of the form
((recordInfo[i],recordInfo[j]), datePrint)
and we need these immediately sorted+binned into reducers.

each "recordInfo" is of the form
(fhrDocId,datePrints)

'''
def jaccard(a, b):
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))



def reduce(recordEdge, datePrintIter, context):
    # recordEdge is (recordInfo_i,recordInfo_j)
    # recordEdge[0][1] is the datePrints in record_i
    # recordEdge[1][1] is the datePrints in record_j
    # datePrintIter contains the intersection of days in both records
    union = float(len(set(recordEdge[0][1]).union(recordEdge[1][1])))
    intersection = float(len( sum(1 for _ in datePrintIter) ))
    context.write(recordEdge,intersection/union)







