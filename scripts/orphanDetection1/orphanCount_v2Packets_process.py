import json
import jydoop
import random


'''
in following commands, UPDATE DATES

----to run against HDFS sample
make ARGS="scripts/orphanCount_v2Packets_process.py ./outData/orphanCount_v2Packets_processed_2013-10-31.csv /user/bcolloran/outData/orphanCount_v2Packets_2013-10-31/part*" hadoop

'''

#needed to get data from HDFS
setupjob=jydoop.setupjob



def map(key, value, context):
    # key,vals will be of the form :
    #     (EXCEPTION CODE, count)
    #     (fingerprint, count)



    if key in ["parse_failed","no_payload_version","not_v2","no_thisPingDate","no_dataDays","no_dayWithAppSession"]: # if we get an error codes, count them up
        context.write(key,value)
        return

    #otherwise, the key/val will be:
    #    (fingerprint, count)
    # need to flip this to get, for each N, number of records belonging to a fingerprint with N associated records
    context.write(value,1)



combine = jydoop.sumreducer
reduce = jydoop.sumreducer

