import json
import jydoop
import healthreportutils_v3
import random

'''

----to run against HDFS sample of ALL v3 records (extract created by anurag in HDFS at /user/aphadke/temp_fennec_raw_dump)


---HDFS dump of all Fennec records to HDFS summary
make ARGS="scripts/fennecQuery_DaysEnviromentsMemValues.py ./outData/fennecQuery_DaysEnviromentsMemValues_2013-10-31 /user/aphadke/temp_fennec_raw_dump/part*" hadoop
'''


#needed to get data from HDFS
setupjob=jydoop.setupjob

#don't save to HDFS


@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    numEnvir = len(payload.environments.keys())
    numDays = len(payload.days)


    memValsList = [environment.get("org.mozilla.sysinfo.sysinfo",{}).get("memoryMB",None) for envHash,environment in payload.environments.items() if environment.get("org.mozilla.sysinfo.sysinfo",{}).get("memoryMB",None)]

    print memValsList

    context.write((numDays,numEnvir,len(memValsList),len(set(memValsList))),1)


combine = jydoop.sumreducer
reduce = jydoop.sumreducer





