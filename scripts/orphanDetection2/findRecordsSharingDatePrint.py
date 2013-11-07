import json
import healthreportutils

'''
in following commands, UPDATE DATES

----to run against full HBASE, output to HDFS
jydoopRemote peach scripts/findRecordsSharingDatePrint.py outData/findRecordsSharingDatePrint_test


----to run against full HDFS sample, output to HDFS; on peach:
make ARGS="scripts/orphanDetection2/findRecordsSharingDatePrint.py ./outData/recordsSharingDatePrint /user/bcolloran/outData/v2Packets_sampleWithLinkedOrphans_2013-11-05_v2/part-r-*" hadoop


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






# need to use this since python dicts don't guarantee order, and since json.dumps with sorting flag is broken in jydoop
def dictToSortedTupList(objIn):
    if isinstance(objIn,dict):
        return [(key,dictToSortedTupList(val)) for key,val in sorted(objIn.items(),key=lambda item:item[0])]
    else:
        return objIn



def map(fhrDocId, rawJsonIn, context):

    try:
        payload = json.loads(rawJsonIn)
    except:
        return

    try:
        payloadVersion = payload["version"]
    except: #was getting errors finding packets without a version
        return

    if payloadVersion != 2:
        return

    #NOTE: we drop any packet without data.days entries. these cannot be fingerprinted/linked.
    try:
        dataDays = payload["data"]["days"].keys()
    except:
        return



    #NOTE: we will use profile creation date to add further refinement to date colisions, but it is not required.
    try:
        profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    except:
        profileCreation = "no_profileCreation"


    datePrints = tuple([ (date,hash(str(dictToSortedTupList(payload["data"]["days"][date])))) for date in payload["data"]["days"].keys() ])
    
    for d in datePrints:
        # print (d,profileCreation)
        # print (fhrDocId,datePrints)
        context.write((d,profileCreation),(fhrDocId,datePrints))


def reduce(datePrint, vIter, context):
    recordInfoList = sorted(list(set(vIter)),key=lambda tup:tup[0])
    for i in range(len(recordInfoList)):
        for j in range(i+1,len(recordInfoList)):
            context.write((recordInfoList[i],recordInfoList[j]),datePrint)





def outputWithKey(path, results):
    """
    Output key/values 
    """
    f = open(path, 'w')
    w = csv.writer(f)
    for k, v in results:
        w.writerow([str(k),str(v)])


def outputWithoutKey(path, results):
    """
    Output key/values
    """
    f = open(path, 'w')
    w = csv.writer(f)
    for k, v in results:
        w.writerow([str(k),str(v)])