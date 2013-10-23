import json
import jydoop
import healthreportutils
import random

'''
version notes:
2013-10-22: require "appSessions" field on fhrActivationDates
'''

setupjob = healthreportutils.setupjob


minimalActiveFhrDaysEntrySet = set(["org.mozilla.crashes.crashes","org.mozilla.appSessions.previous"])

def map(key, value, context):

    try:
        payload = json.loads(value)
    except KeyError:
        #context.write(("error","bad_payload"),(1,"no_build"))
        #context.write("global_count",(1,"global_count"))
        return

    try:
        if payload["version"]<2:
            return
    except KeyError: #was getting errors finding packets without a version field
        return

    try:
        thisPingDate = payload["thisPingDate"]
    except KeyError:
        #context.write(("error","no_thisPingDate"),(1,"no_build"))
        #context.write("global_count",(1,"global_count"))
        return

    try:
        lastPingDate = payload["lastPingDate"]
    except KeyError:
        #context.write(("error","no_thisPingDate"),(1,"no_build"))
        #context.write("global_count",(1,"global_count"))
        lastPingDate = None


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
        buildIdAt_thisPingDate = payload["geckoAppInfo"]["appBuildID"].strip()
    except:
        try:
            buildIdAt_thisPingDate = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["appBuildID"].strip()
        except:
            buildIdAt_thisPingDate='no_buildIdAt_thisPingDate'


    try:
        os = payload["geckoAppInfo"]["os"]
    except KeyError:
        try:
            os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
        except KeyError:
            os = "no_os"


    try:
        profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    except KeyError:
        profileCreation = "no_profileCreation"


    try:
        country =payload["geoCountry"]
    except KeyError:
        country="no_country"

    try:
        memory =payload["data"]["last"]["org.mozilla.sysinfo.sysinfo"]["memoryMB"]
    except KeyError:
        memory="no_memory"


    try:
        dataDays = payload["data"]["days"].keys()
    except KeyError:
        dataDays = None


    #set up the complete list of observed active days. We will need to go back and remove entries from before the FHR activation date.
    activeDays = [thisPingDate]
    activeDays = activeDays+dataDays if dataDays else activeDays
    activeDays = activeDays+[lastPingDate] if lastPingDate else activeDays



    #find the FHR activation date
    if dataDays:
        #if there are dataDays entries, get the first one on which FHR is active and has a session recorded
        try:
            fhrActiveDataDaysWithSessionList = [day for day in dataDays if (not (set(payload["data"]["days"][day].keys())<=minimalActiveFhrDaysEntrySet)) and ("org.mozilla.appSessions.previous" in payload["data"]["days"][day].keys() )]
        except AttributeError:
            # was getting "AttributeError: 'unicode' object has no attribute 'keys'"
            return
        # if there are fhr active days, find the first of them; if there are no entries in dataDays that have these fields, perhaps we are looking at an instance that has just had FHR activated, and has some old crashes? in the latter case get the earliest day in activeDays
        if fhrActiveDataDaysWithSessionList:
            fhrActivationDate=min(fhrActiveDataDaysWithSessionList)
        else:
            fhrActivationDate = min(activeDays)
    else:
        #if there are not any dataDays entries, get the earliest day in activeDays, which will be the min of thisPingDate and lastPingDate (and hopefully lastPingDate should not exist in this case...)
        fhrActiveDataDaysWithSessionList = []
        fhrActivationDate = min(activeDays)

    #find the days active since FHR code became active.
    activeDaysSinceFhr = [date for date in activeDays if date>=fhrActivationDate]



    #find the build transition dates. these dates are necessarily a subset of the fhrActiveDataDaysWithSessionList, so going off of that is enough. if there are no days in fhrActiveDataDaysWithSessionList, then ascribe all activeDaysSinceFhr to the buildIdAt_thisPingDate, and set transition date in to be the fhrActivationDate
    if fhrActiveDataDaysWithSessionList:
        # appBuildIdTransitions is a list of tuples (buildId, transitionDateIn)
        appBuildIdTransitions=[]
        try:
            for day in fhrActiveDataDaysWithSessionList:
                if "org.mozilla.appInfo.versions" in payload["data"]["days"][day].keys():
                    if "appBuildID" in payload["data"]["days"][day]["org.mozilla.appInfo.versions"]:
                        appBuildIdTransitions+=[(payload["data"]["days"][day]["org.mozilla.appInfo.versions"]["appBuildID"][0], day)]
                    else:
                        return
                    # except KeyError:
                    #     raise Exception( str(payload["data"]["days"][day]["org.mozilla.appInfo.versions"])+"   "+str(payload["version"]) )
        except:
            return


        if appBuildIdTransitions:
            #if there are any appBuildIdTransitions observed, sort them by day
            appBuildIdTransitions = sorted(appBuildIdTransitions,key=lambda tup: tup[1])
        else:
            #if there are NO appBuildIdTransitions observed, then ascribe all activeDaysSinceFhr to the buildIdAt_thisPingDate, and set the transition date in to be the fhrActivationDate
            appBuildIdTransitions = [(buildIdAt_thisPingDate,fhrActivationDate)]
    else:
        #if there are no days in fhrActiveDataDaysWithSessionList, then ascribe all activeDaysSinceFhr to the buildIdAt_thisPingDate, and set transition date in to be the fhrActivationDate
        appBuildIdTransitions = [(buildIdAt_thisPingDate,fhrActivationDate)]


    #now that we have the sorted appBuildIdTransitions, we can go back and attribute the observed activeDaysSinceFhr to each build.
    #appBuildIdTransitionsWithNumDaysActive will be a list of tuples (buildId,transitionDateIn,daysActiveOnBuild)
    appBuildIdTransitionsWithNumDaysActive = []
    #for all but the last buildId transition
    for i,transitionDateAndBuildId in enumerate(appBuildIdTransitions[0:-1]):
        transitionDateIn = appBuildIdTransitions[i][1]
        transitionDateOut = appBuildIdTransitions[i+1][1]
        thisBuildId = appBuildIdTransitions[i][0]

        daysActiveOnBuild = len([day for day in activeDaysSinceFhr if (transitionDateIn<=day and day<transitionDateOut)])
        
        appBuildIdTransitionsWithNumDaysActive += [ (thisBuildId,transitionDateIn,daysActiveOnBuild) ]
    #for the last buildId transition
    #if appBuildIdTransitions:
    lastTransitionDateIn = appBuildIdTransitions[-1][1]
    lastBuildId = appBuildIdTransitions[-1][0]

    daysActiveOnBuild = len([day for day in activeDaysSinceFhr if (lastTransitionDateIn<=day)])
    appBuildIdTransitionsWithNumDaysActive += [ (lastBuildId,lastTransitionDateIn,daysActiveOnBuild) ]





    try:
        firstFhrActiveDayData = str( payload["data"]["days"][min(activeDaysSinceFhr)] )
    except KeyError:
        firstFhrActiveDayData = "no_firstFhrActiveDayData"

    # print appBuildIdTransitionsWithNumDaysActive

    context.write(  (os,
                  updateChannel,
                  country,
                  str(memory),
                  str(profileCreation),
                  min(activeDaysSinceFhr),
                  fhrActivationDate,
                  firstFhrActiveDayData)

                  ,(thisPingDate,appBuildIdTransitionsWithNumDaysActive)  )













def reduce(k, vIter, cx):


    #infoPerBuildId is a dict of dicts with entries like :
    # {BUILD_ID: {"dateIn":DATE, "daysActive":INT,"orphaningVect":SPARSE_VECT}}
    infoPerBuildId = dict()
    
    thisPingDateCounter={}
    # iterate across the records associated with this fingerprint, aggregating data as we go
    for val in vIter:
        #increment the thisPingDateCounter
        if val[0] in thisPingDateCounter.keys():
            thisPingDateCounter[val[0]]+=1
        else:
            thisPingDateCounter[val[0]]=1

        for thisBuild,thisTransitionDateIn,thisDaysActiveOnBuild in val[1]:
            # if the thisBuild is already in infoPerBuildId,
            # (1) check that the dates of the transition in to the build are the same. if not, emit an anomaly code.
            # (2) set the days active to be the max of the days active in this record and the ones seen so far
            if thisBuild in infoPerBuildId.keys():
                if infoPerBuildId[thisBuild]["dateIn"]!=thisTransitionDateIn:
                    cx.write( k[0:3], "MISMATCHED_TRANSITION_DATES" )
                    return

                infoPerBuildId[thisBuild]["daysActive"] = max( [infoPerBuildId[thisBuild]["daysActive"],thisDaysActiveOnBuild] )
            else: #if the build is not yet present add it
                infoPerBuildId[thisBuild]={"dateIn":thisTransitionDateIn, "daysActive":thisDaysActiveOnBuild,"orphaningVect":[]}

    #now go through the thisPingDateList and assign the dates to builds
    buildEntryDateList_sorted = sorted( [(item[1]["dateIn"],item[0]) for item in infoPerBuildId.items()],
                                key= lambda tup:tup[0] )

    #iterate over ping dates and sort them into builds, adding that counter to the (sparse) vector of orphanings on each day under the build in question
    #before doing this, decrement the counter of the latest thisPingDate by 1, to reflect that the latest ping is considered the "head" record, and so not counted as an orphan.
    thisPingDateCounter[max(thisPingDateCounter.keys())]-=1
    for pingDate in thisPingDateCounter.keys():
        #since the buildEntryDateList_sorted is sorted, we can just step through the REVERSED list of dates until we find the first one where pingDate>entryDate, and then add that counter entry to the list of orphanings for the correspondind build.
        #we have to reverse the list so that we are going from newest build to oldest; then the first time pingDate>entryDate
        for entryDate, buildId in reversed(buildEntryDateList_sorted):
            if pingDate>entryDate:
                #add the orphaning info for this build (if the # orphans > 0)
                if thisPingDateCounter[pingDate]>0:
                    infoPerBuildId[buildId]["orphaningVect"]+=[thisPingDateCounter[pingDate]]
                break

    for buildId,infoDict in infoPerBuildId.items():
        if infoDict["daysActive"]<len(infoDict["orphaningVect"]):
            cx.write( k[0:3], "MORE_ORPHAN_DATES_THAN_ACTIVE_DAYS" )
            return






    #print appBuildIdTransitions
    cx.write( k[0:3], str(infoPerBuildId) )

    # if numRecordsThisFingerprint>0:
    #     cx.write( k, 1)












