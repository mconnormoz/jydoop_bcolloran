import json
import jydoop
import healthreportutils_v3
import random

'''

----to run against HDFS sample of ALL v3 records (extract created by anurag in HDFS at /user/aphadke/temp_fennec_raw_dump)


---HDFS dump of all Fennec records to HDFS summary
make ARGS="scripts/fennecQuery_DaysEnvironmentsMemValues.py ./outData/fennecQuery_DaysEnviromentsMemValues_2013-10-31 /user/aphadke/temp_fennec_raw_dump/part*" hadoop
'''


#needed to get data from HDFS *as text*
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


#don't save to HDFS, write as csv


@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    numEnvir = len(payload.environments.keys())
    numDays = len(payload.days)


    memValsList = [environment.get("org.mozilla.sysinfo.sysinfo",{}).get("memoryMB",None) for envHash,environment in payload.environments.items() if environment.get("org.mozilla.sysinfo.sysinfo",{}).get("memoryMB",None)]

    print memValsList

    context.write((numDays,numEnvir,len(memValsList),len(set(memValsList))),1)


combine = jydoop.sumreducer
reduce = jydoop.sumreducer





