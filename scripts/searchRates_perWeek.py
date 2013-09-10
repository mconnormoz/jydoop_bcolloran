import json
import jydoop
import healthreportutils
import sequencefileutils
import random
import csv
import datetime




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


# (startDate + datetime.timedelta(days=7)).strftime("%Y-%m-%d")





startDate = datetime.date(2013,07,01) # Monday, July 1 2013. Using M-Su weeks
numWeeks = 10
weekEndPoints = [startDate.strftime("%Y-%m-%d")]+[(startDate + datetime.timedelta(days=weekNum*7)).strftime("%Y-%m-%d") for weekNum in range(1,numWeeks)
]
print weekEndPoints

@healthreportutils.FHRMapper(only_major_channels=True)
def map(key, payload, context):
    # payload = healthreportutils.FHRPayload(value)


    for searchCounts in payload.daily_search_counts():
        for weekNum in range(1,numWeeks):
            if weekEndPoints[weekNum-1]<searchCounts[0]<=weekEndPoints[weekNum]:
                context.write((weekNum,searchCounts[1],searchCounts[2]),searchCounts[3])
            else:
                continue



combine = jydoop.sumreducer
reduce = jydoop.sumreducer















