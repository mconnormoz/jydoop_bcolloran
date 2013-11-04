import json
import healthreportutils_v3

'''
in following commands, UPDATE DATES

----to *test* on a sample
make ARGS="scripts/v3Packets_passDocIdTest.py ./outData/orphDocIdTest_v3.csv /user/aphadke/temp_fennec_raw_dump/part-r-00000" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
# def skip_local_output():
#     return True


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




# need to use this since python dicts don't guarantee order, and since json.dumps with sorting flag is broken in jydoop

@healthreportutils_v3.FHRMapper()
def map(docId, payload, context):

    try:
        firstDayData = str(payload._o.get('data', {}).get('days', {}).get(payload.days[0],{}))
        context.write(hash(firstDayData),docId)
    except:
        pass




minDocIdsToReport=1
maxDocIdsToReport=6

def reduce(fingerprint,docIdIter,context):
    numDocIds=0
    for docId in docIdIter:
        numDocIds+=1
        if numDocIds>maxDocIdsToReport:
            return

    if numDocIds>=minDocIdsToReport:
        context.write('',tuple(docIdIter))







