import json
import jydoop
import healthreportutils_v3
import random


'''
in following commands, UPDATE DATES

----to run against HDFS sample
make ARGS="scripts/orphaningDatesFennec_extraInfo_process.py ./outData/orphaningDatesFennec_extraInfo_processed.csv /user/bcolloran/outData/orphaningDatesFennec_extraInfo_2013-10-24/part*
" hadoop

'''

#needed to get data from HDFS
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




def map(key, value, context):
    if key in ["no_firstDay","no_firstDayFirstEnvir","no_firstDayFirstEnvirAppSession","empty_firstDayFirstEnvirAppSession","recordWithUniquePrint"]: # if we get an error codes, count them up
        context.write(key,value)
        return

    #otherwise, the key will be a (firstDay,hash(firstDayDataStr)) tuple fingerprint, and the value will be a list of thisPingDates for each record associated with the fingerprint. there should be at least two thisPingDates
    # emit a "1" for each thisPingDate
    for thisPingDate in sorted(value)[:-1]:
        context.write(thisPingDate,1)



combine = jydoop.sumreducer
reduce = jydoop.sumreducer

