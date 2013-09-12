import json
import jydoop
import healthreportutils
import sequencefileutils
import random
import csv


'''
NOTE: this script can be used to find the priors for the psuedo-bayesian approach too. Fit the distribution of n1/N1 for each record.
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

    if not v23date:
        pass
    else: #for records that have a v23date transition recorded...
        # get the number of searches before and after and the proportion using google before and after.

        n1=0
        N1=0
        n2=0
        N2=0
        for searchCounts in payload.daily_search_counts():
            if searchCounts[0]<v23date:
                N1+=1
                if searchCounts[1]=="google" or searchCounts[1]=="google-jp":
                    n1+=1
            elif searchCounts[0]>v23date:
                N2+=1
                if searchCounts[1]=="google" or searchCounts[1]=="google-jp":
                    n2+=1
            else:
                # what happens ON v23date is ambiguous
                pass

        # p1 = float(n1)/float(N1) if N1>0 else 0
        # p2 = float(n2)/float(N2) if N2>0 else 0

        # print (p1,N1,p2,N2)

        context.write(1,(n1,N1,n2,N2))







# combine = jydoop.sumreducer
# reduce = jydoop.sumreducer















