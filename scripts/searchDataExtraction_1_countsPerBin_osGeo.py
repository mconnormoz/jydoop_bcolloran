import json
import jydoop
import healthreportutils
import sequencefileutils
import random
import csv


#setupjob = healthreportutils.setupjob

setupjob = sequencefileutils.setupjob

# make ARGS="scripts/searchDataExtraction_1_countsPerBin_osGeo.py ./outData/searchCounts_bins-osGeo_2013-07_v1.csv /data/fhr/nopartitions/20130902" hadoop





minimalActiveFhrDaysEntrySet = set(["org.mozilla.crashes.crashes","org.mozilla.appSessions.previous"])

def listOfSearchCountsOnDayWithSearch(searchCountDict):
    return [searchCountDict[key] for key in searchCountDict.keys() if key!="_v"]

def totalSearchDictFromSearchDaysData(searchDaysData):
    searchCountTupList = [(key,searchCountDict[key]) for searchCountDict in searchDaysData for key in searchCountDict.keys() if key!="_v"]
    totalSearchDict={}
    for provider,count in searchCountTupList:
        try:
            totalSearchDict[provider]+=count
        except KeyError:
            totalSearchDict[provider]=count
    return totalSearchDict

startDate = "2013-07-01"
endDate = "2013-08-01"


def map(key, value, context):

    try:
        payload = json.loads(value)
    except KeyError:
        #context.write(("error","bad_payload"),(1,"no_build"))
        #context.write("global_count",(1,"global_count"))
        return

    try:
        if payload["version"]!=2:
            return
    except KeyError: #was getting errors finding packets without a version field
        return


    try:
        os = payload["geckoAppInfo"]["os"]
    except KeyError:
        try:
            os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
        except KeyError:
            os = "no_os"


    try:
        country =payload["geoCountry"]
    except KeyError:
        country="no_country"

    if country not in ["BR","CN","DE","ES","FR","ID","IN","IT","JP","MX","PL","RU","TR","US"]:
        country="OTHER"



    try:
        dataDays = payload["data"]["days"].keys()
    except KeyError:
        return


    #set up the complete list of observed active days. We will need to go back and remove entries from before the FHR activation date.


    #find the FHR activation date
    if dataDays:
        #if there are dataDays entries, get the first one on which FHR is active
        try:
            fhrActiveDataDaysList = [day for day in dataDays if not (set(payload["data"]["days"][day].keys())<=minimalActiveFhrDaysEntrySet)]
        except AttributeError:
            # was getting "AttributeError: 'unicode' object has no attribute 'keys'"
            return
        # if there are fhr active days, find the first of them; if there are no entries in dataDays that have these fields, perhaps we are looking at an instance that has just had FHR activated, and has some old crashes? in the latter case get the earliest day in activeDays
        if fhrActiveDataDaysList:
            fhrActivationDate=min(fhrActiveDataDaysList)
        else:
            return
    else:
        return

    activeDaysInRange = [date for date in fhrActiveDataDaysList if (date>=fhrActivationDate and startDate<=date and date<endDate)]

    numActiveDaysInRange = len(activeDaysInRange)

    #find the days active since FHR code became active.
    daysWithSearchesInRange = [payload['data']['days'][date]['org.mozilla.searches.counts'] for date in activeDaysInRange if date>=fhrActivationDate and 'org.mozilla.searches.counts' in payload['data']['days'][date].keys()]

    # totalSearchesOnDaysWithSearches = [sum(listOfSearchCountsOnDayWithSearch(searchCountDict)) for searchCountDict in daysWithSearches]

    totalSearchesByProvider = totalSearchDictFromSearchDaysData(daysWithSearchesInRange)

    


    #desired output variables: [numberInFacet, numberOfSearchesInFacet, numberOfActiveDaysInFacet]

    #if this record HAD ACTIVITY within the specified time range, add it to the general count:
    if numActiveDaysInRange>0:
        totalNumSearches = sum(totalSearchesByProvider.values())
        context.write( (os,
                      country,
                      "ACTIVE_IN_RANGE")
                      ,(1,totalNumSearches,numActiveDaysInRange) )
        if totalNumSearches>0:
            context.write( (os,
                          country,
                          "ANY_SEARCH_PROVIDER")
                          ,(1,totalNumSearches,numActiveDaysInRange) )

    for searchProvider,numSearches in totalSearchesByProvider.items():
        context.write( (os,
                  country,
                  searchProvider)
                  ,(1,numSearches,numActiveDaysInRange) )









def tupleSummer(k,valIter,context):
    outHistDict=dict()
    outList =[0,0,0]
    for tup in valIter:
        outList[0]+=tup[0]
        outList[1]+=tup[1]
        outList[2]+=tup[2]

    context.write(k,tuple(outList))





combine = tupleSummer#jydoop.sumreducer
reduce = tupleSummer#jydoop.sumreducer

# def output(path,reducerOutput):
#     """
#     Output key/values into a reasonable CSV.

#     All lists/tuples are unwrapped.
#     """
#     f = open(path, 'w')
#     w = csv.writer(f,quoting=csv.QUOTE_ALL)
#     for k, v in reducerOutput:
#         l = []
#         jydoop.unwrap(l, k)
#         # unwrap(l, v)
#         w.writerow(l+[str(dict(v))])


















