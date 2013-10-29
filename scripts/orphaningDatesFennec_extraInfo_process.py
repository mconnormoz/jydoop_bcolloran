import json
import jydoop
import healthreportutils_v3
import random


'''
in following commands, UPDATE DATES

----to run against HDFS sample
make ARGS="scripts/orphaningDatesFennec_extraInfo_process.py ./outData/orphaningDatesFennec_extraInfo_processed.csv /user/bcolloran/outData/orphaningDatesFennec_extraInfo_2013-10-24/part*" hadoop

'''

#needed to get data from HDFS
setupjob=jydoop.setupjob



def map(key, value, context):
    if key in ["no_firstDay","no_firstDayFirstEnvir","no_firstDayFirstEnvirAppSession","empty_firstDayFirstEnvirAppSession","recordWithUniquePrint"]: # if we get an error codes, count them up
        context.write(key,value)
        return

    #otherwise, the key will be a (firstDay,hash(firstDayDataStr)) tuple fingerprint, and the value will be a list of thisPingDates for each record associated with the fingerprint. there should be at least two thisPingDates
    # emit a "1" for each thisPingDate
    for thisPingDate in sorted(list(value))[:-1]:
        context.write(thisPingDate,1)



combine = jydoop.sumreducer
reduce = jydoop.sumreducer

