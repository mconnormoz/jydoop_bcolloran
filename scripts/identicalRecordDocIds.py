import json
import healthreportutils
import jydoop

'''
in following commands, UPDATE DATES

----to run against full HBASE
jydoopRemote peach scripts/v2Packets_docIdsFor2-6RecsPerPrint.py outData/v2Packets_docIdsFor2-6RecsPerPrint_2013-11-01.txt

----to *test* on a sample
make ARGS="scripts/v2Packets_docIdsFor2-6RecsPerPrint.py ./outData/v2Packets_docIdsFor2-6RecsPerPrint_2013-11-01_test.txt /data/fhr/raw/20131101/part-m-08*" hadoop


'''

######## to OUTPUT TO HDFS from RAW HBASE
# def skip_local_output():
#     return True


setupjob = healthreportutils.setupjob




# need to use this since python dicts don't guarantee order, and since json.dumps with sorting flag is broken in jydoop
def dictToSortedTupList(objIn):
    if isinstance(objIn,dict):
        return [(key,dictToSortedTupList(val)) for key,val in sorted(objIn.items(),key=lambda item:item[0])]
    else:
        return objIn



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
        payloadHash = hash(rawJsonIn)
        incrCounter(context,"SUCCESS", "hash_succeeded",1)
    except KeyError:
        context.getCounter("ERRORS", "hash_failed",1)
        return

    try:
        payload = json.loads(rawJsonIn)
        incrCounter(context,"SUCCESS", "parse_succeeded",1)
    except KeyError:
        incrCounter(context,"ERRORS", "parse_failed",1)
        return

    try:
        payloadVersion = payload["version"]
    except KeyError: #was getting errors finding packets without a version
        incrCounter(context,"ERRORS", "no_payload_version",1)
        payloadVersion ="no_version"
        return

    try:
        payload["data"]["days"].keys()
    except:
        context.getCounter("ERRORS", "no_data_days").increment(1)
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
    except KeyError:
        try:
            os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
        except KeyError:
            os = "no_os"


    key = (payloadHash,payloadVersion,os,updateChannel)

    context.write(key,docId)





def reduce(key,docIdIter,context):
    docIdOutList = list(docIdIter)
    if len(docIdOutList)>=2:
        context.write(key,docIdOutList)












