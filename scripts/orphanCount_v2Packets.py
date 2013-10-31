import json
import jydoop
import healthreportutils
import random

'''

OUTOUT: will produce (fingerprint,count) pairs that need to be further processed to get count of each the number of prints with X records per print. Need to save output to HDFS for this to work.

in following commands, UPDATE DATES


----to run against full HBASE
make ARGS="scripts/orphanCount_v2Packets.py ./outData/orphaningDatesFennec_extraInfo_2013-10-29" hadoop



SAVE OUTPUT TO HDFS, then need to PROCESS WITH e.g:
make ARGS="scripts/orphanCount_v2Packets.py ./outData/orphanCount_v2Packets_2013-10-25.csv /user/bcolloran/outData/orph_Fennec_byBuildId_extraInfo_2013-10-25/part*" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
def skip_local_output():
    return True


setupjob = healthreportutils.setupjob



def map(key, value, context):

    ##### TEST FOR ALL FINGERPRINT-RUINING ANOMALIES
    try:
        payload = json.loads(value)
    except KeyError:
        context.write("parse_failed",1)
        return


    try:
        payloadVersion = payload["version"]
    except KeyError: #was getting errors finding packets without a version
        context.write("no_payload_version",1)
        return


    if payloadVersion != 2:
        context.write("not_v2",1)
        return


    try:
        thisPingDate = payload["thisPingDate"]
    except KeyError:
        context.write("no_thisPingDate",1)
        return



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




    fingerprint = hash((os,
                  updateChannel,
                  country,
                  str(memory),
                  str(profileCreation),
                  firstAppSessionDay,
                  json.dumps(firstAppSessionDayData,sort_keys=True)))

    context.write(fingerprint,1)







combine = jydoop.sumreducer
reduce = jydoop.sumreducer








