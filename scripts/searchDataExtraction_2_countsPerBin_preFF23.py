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

    ####find the date of the transition to version 23.0
    #if the current version is <23.0, set the v23date to be after any date in the record
    if payload['data']['last']['org.mozilla.appInfo.appinfo']['platformVersion']<"23.0":
        v23date = "9999-99-99"
    else:
        #in this case, the record must be on version 23.0 or higher; initially set the v23date transition date to be before any date in the record, then step through to look for the actual v23date
        v23date="0000-00-00"
        for date in fhrActiveDataDaysList:
            try:
                platformVersion = payload['data']['days'][date]['org.mozilla.appInfo.versions']['platformVersion']
                # print platformVersion
                if platformVersion==["23.0"]:
                    v23date=date
            except KeyError:
                pass
    # print v23date


    activeDaysInRange_preV23 = [date for date in fhrActiveDataDaysList if (date>=fhrActivationDate and startDate<=date and date<endDate and date<v23date)]

    activeDaysInRange_postV23 = [date for date in fhrActiveDataDaysList if (date>=fhrActivationDate and startDate<=date and date<endDate and date>v23date)]

    # print fhrActiveDataDaysList,"\n",activeDaysInRange_preV23,"\n",activeDaysInRange_postV23,"\n"

    numActiveDaysInRange_pre23 = len(activeDaysInRange_preV23)
    numActiveDaysInRange_post23 = len(activeDaysInRange_postV23)



    #note that we skip the actual v23date, since it could have searches from both versions in unknown proportions
    daysWithSearchesInRange_preV23 = [payload['data']['days'][date]['org.mozilla.searches.counts'] for date in activeDaysInRange_preV23 if date>=fhrActivationDate and 'org.mozilla.searches.counts' in payload['data']['days'][date].keys()]

    daysWithSearchesInRange_postV23 = [payload['data']['days'][date]['org.mozilla.searches.counts'] for date in activeDaysInRange_postV23 if date>=fhrActivationDate and 'org.mozilla.searches.counts' in payload['data']['days'][date].keys()]
    # print daysWithSearchesInRange

    totalSearchesByProvider_preV23 = totalSearchDictFromSearchDaysData(daysWithSearchesInRange_preV23)
    totalSearchesByProvider_postV23 = totalSearchDictFromSearchDaysData(daysWithSearchesInRange_postV23)
    # print totalSearchesByProvider

    


    #desired output variables: [numberInFacet, numberOfSearchesInFacet, numberOfActiveDaysInFacet]

    #if this record HAD ACTIVITY within the specified time range, add it to the general count:
    for versionFlag,totalSearchesByProvider,numActiveDaysInRange in [("pre23",totalSearchesByProvider_preV23,numActiveDaysInRange_pre23),("23+",totalSearchesByProvider_postV23,numActiveDaysInRange_post23)]:
        if numActiveDaysInRange>0:
            totalNumSearches = sum(totalSearchesByProvider.values())
            context.write( (os,
                          updateChannel,
                          country,
                          versionFlag,
                          "ACTIVE",
                          "ACTIVE")
                          ,(1,totalNumSearches,numActiveDaysInRange) )
            if totalNumSearches>0:
                context.write( (os,
                              updateChannel,
                              country,
                              versionFlag,
                              "ANY_PROVIDER",
                              "ANY_LOCATION")
                              ,(1,totalNumSearches,numActiveDaysInRange) )

        for searchTup,numSearches in totalSearchesByProvider.items():
            searchProvider = searchTup[0]
            searchLocation = searchTup[1]
            context.write( (os,
                      updateChannel,
                      country,
                      versionFlag,
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


















