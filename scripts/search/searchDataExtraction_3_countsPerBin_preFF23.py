import json
import jydoop
import healthreportutils
import sequencefileutils
import random
import csv




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






# make ARGS="scripts/searchDataExtraction_2_countsPerBin_preFF23.py ./outData/searchCounts_bins-preFF23_2013-07_v1.csv /data/fhr/nopartitions/20130902" hadoop


@healthreportutils.FHRMapper(only_major_channels=True)
def map(key, payload, context):
    # payload = healthreportutils.FHRPayload(value)


    #iterate over the version info for days that have ['org.mozilla.appInfo.versions']["appVersion"], and return the date on which the transition to 23 occurs. Initialize the date as a 'None'
    v23date=None
    for date,versionInfo in (dayVersionTup for dayVersionTup in payload.daily_provider_data('org.mozilla.appInfo.versions') if "appVersion" in dayVersionTup[1].keys()):
        try:
            #was getting "AttributeError: 'int' object has no attribute 'split'"
            versionMajorString=versionInfo["appVersion"][0].split(".")[0]
        except AttributeError:
            versionMajorString=str(versionInfo["appVersion"][0])

        if versionMajorString=="23":
            v23date=date

    #only consider records that have a v23date transition recorded
    if v23date and payload.channel=='release':


        # we want to know the rate of searches/provider/activeTick on >=23 vs. the rate on <23.
        # also want to know the rate of searches/provider/activeDay on >=23 vs. the rate on <23.
        #to calculate this, get the number of ticks and active days on v <23 for all actives with a v23date, and #ticks #activeDays on v >=23
        # emit key,val pairs:
        #    k=(versionFlag,searchProvider,SAP),v=(countThisRecord)

        #NOTE only consider days STRICTLY greater or less than the v23date-- what happens ON the v23date is ambiguous.

        context.write(("<23","ACTIVITY","DAYS"),
            len([date for date in payload.days if date<v23date]))
        context.write((">=23","ACTIVITY","DAYS"),
            len([date for date in payload.days if date>v23date]))

        context.write(("<23","ACTIVITY","TICKS"),
            sum([dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]<v23date]))
        context.write((">=23","ACTIVITY","TICKS"),
            sum([dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]>v23date]))

        for searchCounts in payload.daily_search_counts():
            if searchCounts[0]<v23date:
                context.write(("<23",searchCounts[1],searchCounts[2]),searchCounts[3])
            elif searchCounts[0]>v23date:
                context.write((">=23",searchCounts[1],searchCounts[2]),searchCounts[3])
            else:
                continue



combine = jydoop.sumreducer
reduce = jydoop.sumreducer















