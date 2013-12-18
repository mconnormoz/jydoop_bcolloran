import json
import healthreportutils
import jydoop
import hashlib

'''
in following commands, UPDATE DATES

----to run against full HBASE
jydoopRemote scripts/identicalRecordDocIds.py outData/docIdsOfIdenticalRecords_{/DATE/}.txt


'''

######## to OUTPUT TO HDFS from RAW HBASE
# def skip_local_output():
#     return True


setupjob = healthreportutils.setupjob




def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)


def incrCounter(context,group,label,value):
    if jydoop.isJython():
        context.getCounter(group,label).increment(value)
    else:
        pass



def map(docId, rawJsonIn, context):

    ##### TEST FOR ALL FINGERPRINT-RUINING ANOMALIES
    try:
        payloadHash = hashlib.md5(rawJsonIn).hexdigest()
        incrCounter(context,"SUCCESS", "hash_succeeded",1)
    except KeyError:
        context.getCounter("ERRORS", "hash_failed",1)
        return

    try:
        payload = json.loads(rawJsonIn)
        incrCounter(context,"SUCCESS", "parse_succeeded",1)
    except KeyError:
        incrCounter(context,"ERRORS", "parse_failed",1)

    try:
        payloadVersion = payload["version"]
    except KeyError: #was getting errors finding packets without a version
        incrCounter(context,"ERRORS", "no_payload_version",1)
        payloadVersion ="no_version"

    try:
        payload["data"]["days"].keys()
    except:
        incrCounter(context,"ERRORS", "no_data_days",1)

    try: #channel
      updateChannel = payload["geckoAppInfo"]["updateChannel"].strip()
    except:
        try:
            updateChannel = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["updateChannel"].strip()
        except:
            updateChannel='no_channel'


    try:
        os = payload["geckoAppInfo"]["os"]
    except KeyError:
        try:
            os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
        except KeyError:
            os = "no_os"


    key = (payloadHash,payloadVersion,os,updateChannel)

    context.write(key,docId)





def reduce(key,docIdIter,context):
    docIdOutList = list(docIdIter)
    incrCounter(context,"STATS", "unique_hashes",1)
    if len(docIdOutList)>=2:
        incrCounter(context,"STATS", "hashes_with_>1_record",1)
        context.write(key,docIdOutList)












