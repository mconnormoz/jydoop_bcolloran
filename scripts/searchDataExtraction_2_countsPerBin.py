import json
import jydoop
import healthreportutils
import sequencefileutils
import random
import csv


#setupjob = healthreportutils.setupjob

setupjob = sequencefileutils.setupjob






minimalActiveFhrDaysEntrySet = set(["org.mozilla.crashes.crashes","org.mozilla.appSessions.previous"])

def listOfSearchCountsOnDayWithSearch(searchCountDict):
    return [searchCountDict[key] for key in searchCountDict.keys() if key!="_v"]

def totalSearchDictFromSearchDaysData(searchDaysData):
    searchCountTupList = [(tuple(searchProv.split(".")),searchCountDict[searchProv]) for searchCountDict in searchDaysData for searchProv in searchCountDict.keys() if searchProv!="_v"]
    totalSearchDict={}
    for providerTup,count in searchCountTupList:
        try:
            totalSearchDict[providerTup]+=count
        except KeyError:
            totalSearchDict[providerTup]=count
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


    try: #channel
      updateChannel = payload["geckoAppInfo"]["updateChannel"].strip()
    except:
        try:
            updateChannel = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["updateChannel"].strip()
        except:
            updateChannel='no_channel'
    if not (updateChannel in ["nightly","aurora","beta","release"]):
        #context.write(("error","wrong_channel"),(1,"no_build"))
        #context.write("global_count",(1,"global_count"))
        return


    try:
        os = payload["geckoAppInfo"]["os"]
    except KeyError:
        try:
            os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
        except KeyError:
            os = "no_os"


    # try:
    #     profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    # except KeyError:
    #     profilecreation = "no_profilecreation"


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


    daysWithSearchesInRange = [payload['data']['days'][date]['org.mozilla.searches.counts'] for date in activeDaysInRange if date>=fhrActivationDate and 'org.mozilla.searches.counts' in payload['data']['days'][date].keys()]
    # print daysWithSearchesInRange

    totalSearchesByProvider = totalSearchDictFromSearchDaysData(daysWithSearchesInRange)
    # print totalSearchesByProvider

    


    #desired output variables: [numberInFacet, numberOfSearchesInFacet, numberOfActiveDaysInFacet]

    #if this record HAD ACTIVITY within the specified time range, add it to the general count:
    if numActiveDaysInRange>0:
        totalNumSearches = sum(totalSearchesByProvider.values())
        context.write( (os,
                      updateChannel,
                      country,
                      "ACTIVE",
                      "ACTIVE")
                      ,(1,totalNumSearches,numActiveDaysInRange) )
        if totalNumSearches>0:
            context.write( (os,
                          updateChannel,
                          country,
                          "ANY_PROVIDER",
                          "ANY_LOCATION")
                          ,(1,totalNumSearches,numActiveDaysInRange) )

    for searchTup,numSearches in totalSearchesByProvider.items():
        searchProvider = searchTup[0]
        searchLocation = searchTup[1]
        context.write( (os,
                  updateChannel,
                  country,
                  searchProvider,
                  searchLocation)
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


















