'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/getJaccardWeightsAndInitParts.py ./outData/weightedEdgesInParts /user/bcolloran/outData/recordsSharingDatePrint/part-r*" hadoop

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




def map(key,val,context):
    context.write(str(key),str(val))