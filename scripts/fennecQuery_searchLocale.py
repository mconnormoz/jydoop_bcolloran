import json
import jydoop
import healthreportutils_v3
import random





setupjob = healthreportutils_v3.setupjob

@healthreportutils_v3.FHRMapper()
def map(key, payload, context):

    searchCounts = payload.daily_search_counts()

    for searchCount in searchCounts:
        context.write(searchCount[0:3], searchCount[3])



reduce = jydoop.sumreducer





