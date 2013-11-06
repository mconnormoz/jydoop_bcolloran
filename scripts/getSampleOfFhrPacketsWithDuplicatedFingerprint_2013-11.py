import json
import healthreportutils
import random

'''
in following commands, UPDATE DATES

----to run against full HBASE, output to HDFS
jydoopRemote peach scripts/getSampleOfFhrPacketsWithDuplicatedFingerprint_2013-11.py outData/v2Packets_sampleWithLinkedOrphans_2013-11-05

'''

######## to OUTPUT TO HDFS from RAW HBASE
def skip_local_output():
    return True


# setupjob = healthreportutils.setupjob

def setupjob(job, args):
    """
    Set up a job to run full table scans for FHR data.

    We don't expect any arguments.
    """

    import org.apache.hadoop.hbase.client.Scan as Scan
    import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil as MSTMRU

    scan = Scan()
    scan.setCaching(500)
    scan.setCacheBlocks(False)
    scan.addColumn(bytearray('data'), bytearray('json'))

    # FIXME: do it without this multi-scan util
    scans = [scan]
    MSTMRU.initMultiScanTableMapperJob(
        'metrics', scans,
        None, None, None, job)

    # inform HadoopDriver about the columns we expect to receive
    job.getConfiguration().set("org.mozilla.jydoop.hbasecolumns", "data:json");
    job.getConfiguration().set("mapred.reduce.tasks","4352")


'''
As of 2013-11-01 ish, there were ~366*10^6 FINGERPRINTS in HBASE. Sample 1/1000 fingerprints to get ~366,000 fingerprints, which should come out to something like 1M maybe records
'''
sampleRate = 0.001



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


    try:
        dataDays = payload["data"]["days"].keys()
    except:
        return


    firstAppSessionDay=None
    for day in sorted(dataDays):
        try:
            if "org.mozilla.appSessions.previous" in payload["data"]["days"][day].keys():
                firstAppSessionDay = day
        except:
            pass
        if firstAppSessionDay:
            break

    if firstAppSessionDay:
        firstAppSessionDayData = payload["data"]["days"][firstAppSessionDay]
    else:
        return




    try: #channel
      updateChannel = payload["geckoAppInfo"]["updateChannel"].strip()
    except:
        try:
            updateChannel = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["updateChannel"].strip()
        except:
            updateChannel='no_channel'


    try:
        os = payload["geckoAppInfo"]["os"]
    except:
        try:
            os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
        except:
            os = "no_os"


    try:
        profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    except:
        profileCreation = "no_profileCreation"


    try:
        country =payload["geoCountry"]
    except:
        country="no_country"


    try:
        memory =payload["data"]["last"]["org.mozilla.sysinfo.sysinfo"]["memoryMB"]
    except:
        memory="no_memory"



    firstAppSessionDayDataStr=str(dictToSortedTupList(firstAppSessionDayData))
    #print firstAppSessionDayDataStr

    fingerprint = hash((os,
                  updateChannel,
                  country,
                  str(memory),
                  str(profileCreation),
                  firstAppSessionDay,
                  firstAppSessionDayDataStr))

    context.write(fingerprint,(fhrDocId,rawJsonIn))





def reduce(fingerprint, vIter, context):

    if random.random()>sampleRate:
        #sampling
        return

    i=0
    recordList=[]
    for fhrDocId,rawJsonIn in vIter:
        if i==0:
            zerothDocId=fhrDocId
            zerothRecord=rawJsonIn
        elif i==1:
            #don't write out any records until there are at least 2 found for the fingerprint
            context.write(zerothDocId,zerothRecord)
            context.write(fhrDocId,rawJsonIn)
        elif i>1 and i<25:
            context.write(fhrDocId,rawJsonIn)
        else:
            #to save memory, we only get the first 25 records for a fingerprint
            return

        i+=1










