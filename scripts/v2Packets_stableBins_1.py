import json
import jydoop
import healthreportutils
import random
from collections import OrderedDict

'''

OUTOUT: will produce (fingerprint,count) pairs that need to be further processed to get count of each the number of prints with X records per print. Need to save output to HDFS for this to work.

in following commands, UPDATE DATES


----to run against full HBASE
make ARGS="scripts/orphanCount_v2Packets.py ./outData/orphaningDatesFennec_extraInfo_2013-10-29" hadoop



SAVE OUTPUT TO HDFS, then need to PROCESS WITH e.g:
make ARGS="scripts/orphanCount_v2Packets.py ./outData/orphanCount_v2Packets_2013-10-25.csv /user/bcolloran/outData/orph_Fennec_byBuildId_extraInfo_2013-10-25/part*" hadoop

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



def map(key, value, context):

    ##### TEST FOR ALL FINGERPRINT-RUINING ANOMALIES
    try:
        payload = json.loads(value)
    except:
        context.write("parse_failed",1)
        return


    try:
        payloadVersion = payload["version"]
    except: #was getting errors finding packets without a version
        context.write("no_payload_version",1)
        return


    if payloadVersion != 2:
        context.write("not_v2",1)
        return

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
        xpcomabi =payload['geckoAppInfo']['xpcomabi']
    except:
        xpcomabi="no_xpcomabi"


    try:
        memory =payload["data"]["last"]["org.mozilla.sysinfo.sysinfo"]["memoryMB"]
    except:
        memory="no_memory"


    try:
        architecture =payload["data"]["last"]["org.mozilla.sysinfo.sysinfo"]["architecture"]
    except:
        architecture="no_architecture"


    try:
        cpuCount =payload["data"]["last"]["org.mozilla.sysinfo.sysinfo"]["cpuCount"]
    except:
        cpuCount="no_cpuCount"







    fingerprint = (os,
                  updateChannel,
                  str(memory),
                  str(profileCreation),
                  str(cpuCount),
                  architecture,
                  xpcomabi)

    context.write(fingerprint,1)







combine = jydoop.sumreducer
reduce = jydoop.sumreducer








