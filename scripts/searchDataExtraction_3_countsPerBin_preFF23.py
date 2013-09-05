import json
import jydoop
import healthreportutils
import sequencefileutils
import random
import csv


def setupjob(job, args):
    """
    Set up a job to run on one or more HDFS locations

    Jobs expect one or more arguments, the HDFS path(s) to the data.
    """

    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat as FileInputFormat
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat as MyInputFormat

    if len(args) < 1:
        raise Exception("Usage: <hdfs-location1> [ <location2> ] [ <location3> ] [ ... ]")

    job.setInputFormatClass(MyInputFormat)
    FileInputFormat.setInputPaths(job, ",".join(args));
    job.getConfiguration().set("org.mozilla.jydoop.mappertype", "TEXT")
    job.getConfiguration().set("mapred.job.queue.name","research")






# make ARGS="scripts/searchDataExtraction_2_countsPerBin_preFF23.py ./outData/searchCounts_bins-preFF23_2013-07_v1.csv /data/fhr/nopartitions/20130902" hadoop


startDate = "2013-06-01"
endDate = "2013-08-01"

@healthreportutils.FHRMapper(only_major_channels=True)
def map(key, payload, context):
    # payload = healthreportutils.FHRPayload(value)


    #iterate over the version info for days that have ['org.mozilla.appInfo.versions']["appVersion"], and return the date on which the transition to 23 occurs. Initialize the date as a 'None'
    v23date=None
    for date,versionInfo in (item for item in payload.daily_provider_data('org.mozilla.appInfo.versions') if "appVersion" in item[1].keys()):
        if versionInfo["appVersion"][0].split(".")[0]=="23":
            v23date=date

    #only consider records that have a v23date transition recorded
    if v23date:
        # print [dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]<v23date]
        # print v23date
        # print [dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]>v23date]
        # # print [searchCounts for searchCounts in payload.daily_search_counts() if searchCounts[0]>v23date]
        # print ""

        # we want to know the rate of searches/provider/activeTick on >=23 vs. the rate on <23.
        # also want to know the rate of searches/provider/activeDay on >=23 vs. the rate on <23.
        #to calculate this, get the number of ticks and active days on v <23 for all actives with a v23date, and #ticks #activeDays on v >=23
        # emit key,val pairs:
        #    k=(versionFlag,searchProvider,SAP),v=(countThisRecord)

        #NOTE only consider days STRICTLY greater or less than the v23date-- what happens ON this date is ambiguous.

        context.write(("<23","ACTIVITY","DAYS"),
            len([date for date in payload.days if date<v23date]))
        context.write((">=23","ACTIVITY","DAYS"),
            len([date for date in payload.days if date>v23date]))

        context.write(("<23","ACTIVITY","TICKS"),
            sum([dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]<v23date]))
        context.write((">=23","ACTIVITY","TICKS"),
            sum([dailySessionInfo[1].active_ticks for dailySessionInfo in payload.session_times() if dailySessionInfo[0]>v23date]))

        for searchCounts in payload.daily_search_counts():
            if searchCounts[0]<v23date:
                context.write(("<23",searchCounts[1],searchCounts[2]),searchCounts[3])
            elif searchCounts[0]>v23date:
                context.write((">=23",searchCounts[1],searchCounts[2]),searchCounts[3])
            else:
                continue

        # for searchCounts in payload.daily_search_counts():
        #     if searchCounts[0]<v23date:
        #         context.write(("<23",searchCounts[1],searchCounts[2]),searchCounts[3])
        #     elif searchCounts[0]>v23date:
        #         context.write((">=23",searchCounts[1],searchCounts[2]),searchCounts[3])
        #     else:
        #         continue






    # print ((item[0],item[1]["appVersion"]) for item in payload.daily_provider_data('org.mozilla.appInfo.versions') if "appVersion" in item[1].keys()),"\n"
    # print [item for item in payload.daily_search_counts()]

    # try:
    #     payload = json.loads(value)
    # except KeyError:
    #     #context.write(("error","bad_payload"),(1,"no_build"))
    #     #context.write("global_count",(1,"global_count"))
    #     return

    # try:
    #     if payload["version"]!=2:
    #         return
    # except KeyError: #was getting errors finding packets without a version field
    #     return


    # try: #channel
    #   updateChannel = payload["geckoAppInfo"]["updateChannel"].strip()
    # except:
    #     try:
    #         updateChannel = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["updateChannel"].strip()
    #     except:
    #         updateChannel='no_channel'
    # if not (updateChannel in ["nightly","aurora","beta","release"]):
    #     #context.write(("error","wrong_channel"),(1,"no_build"))
    #     #context.write("global_count",(1,"global_count"))
    #     return


    # try:
    #     os = payload["geckoAppInfo"]["os"]
    # except KeyError:
    #     try:
    #         os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
    #     except KeyError:
    #         os = "no_os"


    # # try:
    # #     profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    # # except KeyError:
    # #     profilecreation = "no_profilecreation"


    # try:
    #     country =payload["geoCountry"]
    # except KeyError:
    #     country="no_country"

    # if country not in ["BR","CN","DE","ES","FR","ID","IN","IT","JP","MX","PL","RU","TR","US"]:
    #     country="OTHER"



    # try:
    #     dataDays = payload["data"]["days"].keys()
    # except KeyError:
    #     return


    # #set up the complete list of observed active days. We will need to go back and remove entries from before the FHR activation date.


    # #find the FHR activation date
    # if dataDays:
    #     #if there are dataDays entries, get the first one on which FHR is active
    #     try:
    #         fhrActiveDataDaysList = [day for day in dataDays if not (set(payload["data"]["days"][day].keys())<=minimalActiveFhrDaysEntrySet)]
    #     except AttributeError:
    #         # was getting "AttributeError: 'unicode' object has no attribute 'keys'"
    #         return
    #     # if there are fhr active days, find the first of them; if there are no entries in dataDays that have these fields, perhaps we are looking at an instance that has just had FHR activated, and has some old crashes? in the latter case get the earliest day in activeDays
    #     if fhrActiveDataDaysList:
    #         fhrActivationDate=min(fhrActiveDataDaysList)
    #     else:
    #         return
    # else:
    #     return

    # ####find the date of the transition to version 23.0
    # #if the current version is <23.0, set the v23date to be after any date in the record

    # try:
    #     currentVersion = payload["geckoAppInfo"]["version"].strip()
    # except KeyError:
    #     try:
    #         currentVersion = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["version"].strip()
    #     except KeyError:
    #         #if a current version cannot be found, drop packet
    #         return

    # try:
    #     currentVersionMajor = int(currentVersion.split(".")[0])
    # except ValueError:
    #     # if the first part of the version string can't be cast as a float, drop the record
    #     return



    # if currentVersionMajor<23:
    #     # if the currentVersionMajor<23, the v23date transition is AFTER any date in the record
    #     v23date = "9999-99-99"
    # else:
    #     #in this case, the record must be on version 23.0 or higher; initially set the v23date transition date to be BEFORE any date in the record, then step through to look for the actual v23date
    #     v23date="0000-00-00"
    #     for date in fhrActiveDataDaysList:
    #         try:
    #             platformVersion = payload['data']['days'][date]['org.mozilla.appInfo.versions']['platformVersion']
    #             # print platformVersion
    #             if platformVersion==["23.0"]:
    #                 v23date=date
    #         except KeyError:
    #             pass
    # print v23date


    # activeDaysInRange_preV23 = [date for date in fhrActiveDataDaysList if (date>=fhrActivationDate and startDate<=date and date<endDate and date<v23date)]

    # activeDaysInRange_postV23 = [date for date in fhrActiveDataDaysList if (date>=fhrActivationDate and startDate<=date and date<endDate and date>v23date)]

    # print fhrActiveDataDaysList,"\n",activeDaysInRange_preV23,"\n",activeDaysInRange_postV23,"\n"

    # numActiveDaysInRange_pre23 = len(activeDaysInRange_preV23)
    # numActiveDaysInRange_post23 = len(activeDaysInRange_postV23)



    # #note that we skip the actual v23date, since it could have searches from both versions in unknown proportions
    # daysWithSearchesInRange_preV23 = [payload['data']['days'][date]['org.mozilla.searches.counts'] for date in activeDaysInRange_preV23 if date>=fhrActivationDate and 'org.mozilla.searches.counts' in payload['data']['days'][date].keys()]

    # daysWithSearchesInRange_postV23 = [payload['data']['days'][date]['org.mozilla.searches.counts'] for date in activeDaysInRange_postV23 if date>=fhrActivationDate and 'org.mozilla.searches.counts' in payload['data']['days'][date].keys()]
    # print daysWithSearchesInRange_preV23,"\n",daysWithSearchesInRange_postV23

    # totalSearchesByProvider_preV23 = totalSearchDictFromSearchDaysData(daysWithSearchesInRange_preV23)
    # totalSearchesByProvider_postV23 = totalSearchDictFromSearchDaysData(daysWithSearchesInRange_postV23)


    # #desired output variables: [numberInBin, numberOfSearchesInBin, numberOfActiveDaysInBin]

    # #if this record HAD ACTIVITY within the specified time range, add it to the general count:
    # for versionFlag,totalSearchesByProvider,numActiveDaysInRange in [("pre23",totalSearchesByProvider_preV23,numActiveDaysInRange_pre23),("23+",totalSearchesByProvider_postV23,numActiveDaysInRange_post23)]:
    #     if numActiveDaysInRange>0:
    #         totalNumSearches = sum(totalSearchesByProvider.values())
    #         context.write( (os,
    #                       updateChannel,
    #                       country,
    #                       versionFlag,
    #                       "ACTIVE",
    #                       "ACTIVE")
    #                       ,(1,totalNumSearches,numActiveDaysInRange) )
    #         if totalNumSearches>0:
    #             context.write( (os,
    #                           updateChannel,
    #                           country,
    #                           versionFlag,
    #                           "ANY_PROVIDER",
    #                           "ANY_LOCATION")
    #                           ,(1,totalNumSearches,numActiveDaysInRange) )

    #     for searchTup,numSearches in totalSearchesByProvider.items():
    #         try:
    #             #TRY needed b/c I was getting rare errors: "IndexError: index out of range: 1". This indicates existence of rare badly formed search provider strings missing a "."
    #             searchProvider = searchTup[0]
    #             searchLocation = searchTup[1]
    #             context.write( (os,
    #                       updateChannel,
    #                       country,
    #                       versionFlag,
    #                       searchProvider,
    #                       searchLocation)
    #                       ,(1,numSearches,numActiveDaysInRange) )
    #         except IndexError:
    #             pass











def tupleSummer(k,valIter,context):
    outHistDict=dict()
    outList =[0,0,0]
    for tup in valIter:
        outList[0]+=tup[0]
        outList[1]+=tup[1]
        outList[2]+=tup[2]

    context.write(k,tuple(outList))





combine = jydoop.sumreducer
reduce = jydoop.sumreducer

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


















