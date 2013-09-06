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

    #if there is no v23date present in the record, look at the current FF version to set a v23date
    if not v23date:
        currentVersion = payload._o.get("geckoAppInfo").get("version")
        try:
            currentVersionMajor = int(currentVersion.split(".")[0])
        except:
            #if this fails for any reason, something is wrong with the version string in this payload, so don't process the payload
            return
        if currentVersionMajor<23:
            # in this case, the record has not yet upgraded to 23; we assign the upgrade date to be arbitrarily far in the future, to capture all dates in the record on camparison
            v23date="9999-99-99"
        else:
            #in this case, the instance must have started recording data on v23 or higher, so set the v23 date to be less than any possible date in the record
            v23date="0000-00-00"




    # we want to know the rate of searches/provider/activeTick on >=23 vs. the rate on <23.
    # also want to know the rate of searches/provider/activeDay on >=23 vs. the rate on <23.
    #to calculate this, get the number of ticks and active days on v <23 for all actives with a v23date, and #ticks #activeDays on v >=23
    # emit key,val pairs:
    #    k=(versionFlag,searchProvider,SAP),v=(countThisRecord)

    #NOTE only consider days STRICTLY greater or less than the v23date-- what happens ON the v23date is ambiguous.

    # context.write(("<23","ACTIVITY","DAYS"),
    #     len([date for date in payload.days if date<v23date]))
    # context.write((">=23","ACTIVITY","DAYS"),
    #     len([date for date in payload.days if date>v23date]))

    # context.write(("<23","ACTIVITY","TICKS"),
    #     sum([dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]<v23date]))
    # context.write((">=23","ACTIVITY","TICKS"),
    #     sum([dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]>v23date]))

    for searchCounts in payload.daily_search_counts():
        if searchCounts[0]<v23date:
            context.write(("<23",searchCounts[1],searchCounts[2]),searchCounts[3])
        elif searchCounts[0]>v23date:
            context.write((">=23",searchCounts[1],searchCounts[2]),searchCounts[3])
        else:
            continue



combine = jydoop.sumreducer
reduce = jydoop.sumreducer















