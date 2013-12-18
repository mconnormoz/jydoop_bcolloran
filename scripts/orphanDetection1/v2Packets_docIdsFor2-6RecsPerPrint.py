import json
import healthreportutils

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



def map(docId, rawJsonIn, context):

    ##### TEST FOR ALL FINGERPRINT-RUINING ANOMALIES
    try:
        payload = json.loads(rawJsonIn)
    except KeyError:
        context.write("parse_failed",1)
        return


    try:
        payloadVersion = payload["version"]
    except KeyError: #was getting errors finding packets without a version
        context.write("no_payload_version",1)
        return


    # if payloadVersion != 2:
    #     context.write("not_v2",1)
    #     return


    try:
        dataDays = payload["data"]["days"].keys()
    except:
        context.write("no_dataDays",1)
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
        context.write("no_dayWithAppSession",1)
        return



    # if appSessionDays:
    #     firstAppSessionDay = min(appSessionDays)
    #     firstAppSessionDayData = payload["data"]["days"][firstAppSessionDay]
    # else:
    #     context.write("no_dayWithAppSession",1)
    #     return

    ##### TEST FOR ALL FINGERPRINT-RUINING ANOMALIES #### END




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


    try:
        profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    except KeyError:
        profileCreation = "no_profileCreation"


    try:
        country =payload["geoCountry"]
    except KeyError:
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

    context.write(fingerprint,docId)



minDocIdsToReport=2
maxDocIdsToReport=6

def reduce(fingerprint,docIdIter,context):
    numDocIds=0
    docIdOutList = []
    for docId in docIdIter:
        numDocIds+=1
        docIdOutList+=[docId]
        if numDocIds>maxDocIdsToReport:
            return

    

    if numDocIds>=minDocIdsToReport:
        context.write('',docIdOutList)












