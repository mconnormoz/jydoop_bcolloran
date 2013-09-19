import json
import jydoop
import healthreportutils_v3
import random





setupjob = healthreportutils_v3.setupjob

@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    searchCounts = payload.daily_search_counts()

    print searchCounts

    for countData in searchCounts:
        context.write(
            tuple( list(countData[0:3])+[payload.geo] ),
            countData[3])



reduce = jydoop.sumreducer





