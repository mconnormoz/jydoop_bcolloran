import json
import jydoop
import healthreportutils_v3
import random


'''
in following commands, UPDATE DATES

----to run against HDFS sample
make ARGS="scripts/{THIS_FILE}.py ./outData/{OUT_FILE}.csv /data/fhr/nopartitions/{DATE}/3/part*" hadoop

----to run against full HBASE
make ARGS="scripts/orphanCounts_Fennec.py ./outData/orphanCounts_Fennec_2013-10-24.csv" hadoop

'''



'''
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

setupjob = healthreportutils_v3.setupjob



@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    if payload.version!=3:
        return

    try:
        firstDay = payload.days[0]
    except IndexError:
        context.write("no_firstDay",1)
        return



    try:
        firstDayFirstEnvir = payload._o.get('data', {}).get('days', {}).get(firstDay,{}).keys()[0]
    except IndexError:
        context.write("no_firstDayFirstEnvir",1)
        return



    try:
        firstDayFirstEnvirAppSession = payload._o.get('data', {}).get('days', {}).get(firstDay,{}).get(firstDayFirstEnvir,{})["org.mozilla.appSessions"]
    except KeyError:
        context.write("no_firstDayFirstEnvirAppSession",1)
        return



    if firstDayFirstEnvirAppSession=={}:
        context.write("empty_firstDayFirstEnvirAppSession",1)
        return
    else:
        firstDayDataStr = str(payload._o.get('data', {}).get('days', {}).get(firstDay,{}))
        context.write( (firstDay,hash(firstDayDataStr)), payload._o['thisPingDate'])





def reduce(key,vIter,context):

    if key in ["no_firstDay","no_firstDayFirstEnvir","no_firstDayFirstEnvirAppSession","empty_firstDayFirstEnvirAppSession"]: # if we get an error codes, count them up
        context.write(key,sum(vIter))


    else: #if we don't get an error code, if there is more than one thisPingDate associated with the fingerprint, emit all thisPingDates for the fingerprint.
        thisPingDateList = list(vIter)
        if len(thisPingDateList)>1:
            context.write(key,thisPingDateList)
        else:
            context.write("recordWithUniquePrint",1)





