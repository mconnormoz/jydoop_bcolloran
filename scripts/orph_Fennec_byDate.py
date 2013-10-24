import json
import jydoop
import healthreportutils_v3
import random

'''
make ARGS="scripts/orph_Fennec_byDate.py ./outData/orphaningDatesFennec_2013-10-22.csv /data/fhr/nopartitions/20131012/3/part*" hadoop


'''

# uncomment this to write output to hdfs
# def skip_local_output():
#     return True


# setupjob = healthreportutils_v3.setupjob

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






@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    firstDayData = str(payload._o.get('data', {}).get('days', {}).get(payload.days[0],{}))
    context.write(hash(firstDayData),payload._o['thisPingDate'])


def reduce(key,vIter,context):
    thisPingDateList = list(vIter)
    if len(thisPingDateList)>1:
        context.write(len(thisPingDateList)-1,thisPingDateList[0:-1])








