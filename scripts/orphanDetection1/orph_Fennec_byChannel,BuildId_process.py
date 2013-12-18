import json
import jydoop
import random


'''
in following commands, UPDATE DATES

----to run against HDFS sample
make ARGS="scripts/orph_Fennec_byChannel,BuildId_process.py ./outData/orph_Fennec_byChannel,BuildId_processed_2013-10-28.csv /user/bcolloran/outData/orph_Fennec_byChannel,BuildId_2013-10-28/part*" hadoop

'''

#needed to get data from HDFS
setupjob=jydoop.setupjob



def map(key, value, context):
    # key,vals will be of the form :
    #     (channel,"exception_code"), count
    #     (channel,"recordWithUniquePrint"), 1
    #     (channel,"buildId"), listOfBuildIds


    if key[1] in ["no_firstDay","no_firstDayFirstEnvir","no_firstDayFirstEnvirAppSession","empty_firstDayFirstEnvirAppSession","recordWithUniquePrint"]: # if we get an error codes, count them up
        context.write(key,value)
        return

    #otherwise, the key/val will be:
    #    (channel,"buildId"), listOfBuildIds
    # there should be at least two buildIds
    # emit a "1" for each buildId
    for buildId in sorted(list(value))[:-1]:
        context.write((key[0],buildId),1)



combine = jydoop.sumreducer
reduce = jydoop.sumreducer

