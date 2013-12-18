import json
import jydoop
import healthreportutils_v3
import random

'''

make ARGS="scripts/fennecQuery_searchByGeo.py /outData/fennec_searchByGeo.csv /tmp/partitioned_export.7" hadoop

make ARGS="scripts/fennecQuery_searchByGeo.py /outData/fennec_searchByGeo.csv /data/fhr/tmp/3" hadoop

#did not work
make ARGS="scripts/fennecQuery_searchByGeo.py /outData/fennec_searchByGeo.csv /data/fhr/raw" hadoop


make ARGS="scripts/fennecQuery_searchByGeo.py /outData/fennec_searchByGeo.csv /user/sguha/fhr/samples/output/1pct" hadoop

'''
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

    addons = payload.environments['current']['org.mozilla.addons.active']
    addonInfoList = [(addonId,addons[addonId]["foreignInstall"],addons[addonId]["type"],addons[addonId]["userDisabled"]) for addonId in addons.keys() if addonId!="_v"]



    for addonInfo in addonInfoList:
        context.write(addonInfo,1)


combine = jydoop.sumreducer
reduce = jydoop.sumreducer





