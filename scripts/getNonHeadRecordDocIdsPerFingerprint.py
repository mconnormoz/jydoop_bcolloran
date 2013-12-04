import json
import jydoop
import healthreportutils
import random



setupjob = healthreportutils.setupjob

# def skip_local_output():
#     return True

minimalActiveFhrDaysEntrySet = set(["org.mozilla.crashes.crashes","org.mozilla.appSessions.previous"])





'''
MAPPER:
need to get a fingerprint for each record.
along with each fingerprint (as key), pass the following as value:
(1) the complete raw FHR json
(2) the last date observed (thisPingDate) in the record (which will be used in the reducer to pick the head record)
(3) the number of sessions observed in the last day (which will be used to break ties if there are multiple records with the same last date observed)

REDUCER:
to find the head record for each fingerprint:
a) if there is a unique record with a thisPingDate greater than that in any other record with this fingerprint, then that record is head
b) if there are ties in (a), pick the record with the maximum number of sessionsOnThisPingDate
c) if there are ties after (b), pick the record with the maximum currentSessionTime
d) if that still results in a tie, pick one at random (????? not sure about this... could also discard the fingerprint, or try breaking ties with something else)

this will only work correctly for fingerprints that are truly generated by one instance; for other situations (copied profiles) the results will not be correct
'''



def map(fhrDocId, rawJsonIn, context):

    try:
        payload = json.loads(rawJsonIn)
    except KeyError:
        #context.write(("error","bad_payload"),1)
        #context.write("global_count",1)
        return

    try: #was getting errors finding packets without a version field, so had to wrap this test in a try block
        if not (payload["version"]==2):
            return
    except KeyError:
        #context.write(("error","no_version"),1)
        #context.write("global_count",1)
        return

    try:
        thisPingDate = payload["thisPingDate"]
    except KeyError:
        #context.write(("error","no_thisPingDate"),1)
        #context.write("global_count",1)
        return

    try:
        lastPingDate = payload["lastPingDate"]
    except KeyError:
        #context.write(("error","no_lastPingDate"),1)
        #context.write("global_count",1)
        lastPingDate = None


    try:
        # needed .strip because i was finding updateChannel strings with extra whitespace ("\n")
        updateChannel = payload["geckoAppInfo"]["updateChannel"].strip()
    except:
        try:
            updateChannel = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["updateChannel"].strip()
        except:
            updateChannel='no_channel'



    try:
        os = payload["geckoAppInfo"]["os"]
    except (KeyError,TypeError):
        try:
            os = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["os"].strip()
        except (KeyError,TypeError):
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


    # numAppSessionsPreviousOnThisPingDate and currentSessionTime are needed in the reducer to break ties in finding head records 
    try:
        numAppSessionsPreviousOnThisPingDate=len(payload["data"]["days"][thisPingDate]['org.mozilla.appSessions.previous']["main"])
    except KeyError:
        numAppSessionsPreviousOnThisPingDate = 0
    except TypeError:
        #was getting "TypeError: 'float' object is unsubscriptable" errors in the above. this should not happen, and must indicate a bad packet, which we will discard

        return
    try:
        currentSessionTime=payload["data"]["last"]['org.mozilla.appSessions.current']["totalTime"]
    except KeyError:
        currentSessionTime = 0



    #set up the complete list of observed active days. We will need to go back and remove entries from before the FHR activation date.
    activeDays = [thisPingDate]
    activeDays = activeDays+dataDays if dataDays else activeDays
    activeDays = activeDays+[lastPingDate] if lastPingDate else activeDays



    #find the FHR activation date
    if dataDays:
        #if there are dataDays entries, get the first one on which FHR is active
        try: #need to wrap this in a try block. was finding records where payload["data"]["days"][day] was returning a float.
            fhrActiveDataDaysList = [day for day in dataDays if
                ( type(dict())==type(payload["data"]["days"][day])
                and not set(payload["data"]["days"][day].keys())<=minimalActiveFhrDaysEntrySet) ]
        except:
            raise Exception("day: "+str(day)+"       dataDays:"+str(dataDays))
        # if there are fhr active days, find the first of them; if there are no entries in dataDays that have these fields, perhaps we are looking at an instance that has just had FHR activated, and has some old crashes? in the latter case get the earliest day in activeDays
        if fhrActiveDataDaysList:
            fhrActivationDate=min(fhrActiveDataDaysList)
        else:
            fhrActivationDate = min(activeDays)
    else:
        #if there are not any dataDays entries, get the earliest day in activeDays, which will be the min of thisPingDate and lastPingDate (and hopefully lastPingDate should not exist in this case...)
        fhrActiveDataDaysList = []
        fhrActivationDate = min(activeDays)
    #find the days active since FHR code became active.
    activeDaysSinceFhr = [date for date in activeDays if date>=fhrActivationDate]



    try:
        firstFhrActiveDayData = str( payload["data"]["days"][min(activeDaysSinceFhr)] )
    except KeyError:
        firstFhrActiveDayData = "no_firstFhrActiveDayData"


    context.write(  (os,
                  updateChannel,
                  country,
                  str(memory),
                  str(profileCreation),
                  min(activeDaysSinceFhr),
                  fhrActivationDate,
                  firstFhrActiveDayData)

                  ,(thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime, fhrDocId) )














def reduce(k, vIter, cx):
    # randKey = random.random() #SAMPLING ON
    # if randKey < 0.01: #SAMPLING ON
    if True: #SAMPLING OFF
        maxRecordTupSortKey = None #initialize as a None
        maxRecordTupList = None
        maxDate="0000-00-00"
        maxSessionsOnMaxDate=0
        maxSessTimeOnMaxDate=0

        maxRecordTupSortKey = ("0000-00-00",0,0)

        fhrDocIdList = []

        for valTup in vIter:
            fhrDocIdList+=[valTup[3]]
            if valTup[0:3]>maxRecordTupSortKey:
                #if this is the maximal record, update the maxRecordTupSortKey and reset the maxRecordTupList
                maxRecordTupList=[valTup]
                maxRecordTupSortKey=valTup[0:3]
            elif valTup==maxRecordTupSortKey:
                #if this record is tied for maximal record, add it to the list of record tups that tie for max
                maxRecordTupList+=[valTup]


        #not sure why this was happening, but for some record(s) maxRecordTupList was not being set, which means that for all records with the given fingerprint, it must be that:
        # (thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime) < ("0000-00-00",0,0)
        # this should only be possible if there is a bad thisPingDate, in which case we will discard the fingerprint
        if maxRecordTupList:
            if len(maxRecordTupList)==1:
                maxRecordTupOut = maxRecordTupList[0]

            if len(maxRecordTupList)>1:
                print [tup[0:3] for tup in maxRecordTupList],len(maxRecordTupList)
                maxRecordTupOut = random.choice(maxRecordTupList)
                #cx.write( "NON_UNIQUE_HEAD_RECORD" ,maxRecordTup)

            fhrDocIdList.remove(maxRecordTupOut[3])
            if fhrDocIdList:
                for docId in fhrDocIdList:
                    cx.write( docId , 1)
        else:
            return



            











