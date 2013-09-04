import json
import jydoop
import healthreportutils
import random
import csv


setupjob = healthreportutils.setupjob


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
    if not (updateChannel in ["nightly","aurora","beta"]):
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


    #find the days active since FHR code became active.
    daysWithSearches = [payload['data']['days'][date]['org.mozilla.searches.counts'] for date in dataDays if date>=fhrActivationDate and 'org.mozilla.searches.counts' in payload['data']['days'][date].keys()]

    totalSearchesOnDaysWithSearches = [sum(listOfSearchCountsOnDayWithSearch(searchCountDict)) for searchCountDict in daysWithSearches]

    totalSearchesByProviderDict = totalSearchDictFromSearchDaysData(daysWithSearches)
    # print totalSearchesByProviderDict

    # avgNumSearchesPerActiveDay = sum(totalSearchesOnDaysWithSearches)/float(len(fhrActiveDataDaysList))

    numActiveDays = len(fhrActiveDataDaysList)

    context.write( (os,
                  updateChannel,
                  country,
                  "facetCount")
                  ,[(0,1)] )

    for searchProvider,numSearches in totalSearchesByProviderDict.items():
        for allFlagList in [list("0"*(4-len(bin(i)[2:]))+bin(i)[2:]) for i in range(16)]: #this comprenhesion produces all

            context.write( (os,
                      updateChannel,
                      country,
                      searchProvider)
                      ,(numActiveDays,numSearches,1) )











def addHistogramListsReducer(k,valIter,context):
    outHistDict=dict()
    for histTupList in valIter:
        # print histTupList
        for histTup in histTupList:
            # print histTup
            searchesPerDay=histTup[0]
            numRecords=histTup[1]
            # print searchesPerDay
            try:
                outHistDict[searchesPerDay]+=numRecords
            except KeyError:
                outHistDict[searchesPerDay]=numRecords

    context.write(k,outHistDict.items())





combine = addHistogramListsReducer#jydoop.sumreducer
reduce = addHistogramListsReducer#jydoop.sumreducer

def output(path,reducerOutput):
    """
    Output key/values into a reasonable CSV.

    All lists/tuples are unwrapped.
    """
    f = open(path, 'w')
    w = csv.writer(f,quoting=csv.QUOTE_ALL)
    for k, v in reducerOutput:
        l = []
        jydoop.unwrap(l, k)
        # unwrap(l, v)
        w.writerow(l+[str(dict(v))])


















