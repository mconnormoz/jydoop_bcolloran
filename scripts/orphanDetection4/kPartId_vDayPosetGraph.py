import jydoop
import healthreportutils
import json
import datetime

import orphUtils
import poset2


output = orphUtils.outputTabSep


setupjob = orphUtils.hdfsjobByType("JYDOOP")


def sortedDayInfoToDagInfo(sortedDayInfo):
    maxDateInThread = max( info["date"] for  label,info in sortedDayInfo)
    minDateInThread = max( info["date"] for  label,info in sortedDayInfo)
    return [poset2.DayNode(id=label,date=info["date"],minDateInThread=minDateInThread,maxDateInThread=maxDateInThread,data=info["data"]) for label,info in sortedDayInfo]


@orphUtils.localTextInput()
# @healthreportutils.FHRMapper()
def map(partId,fhrPayload,context):
    #days are sorted by date by .daily_data()
    fhrPayload=json.loads(fhrPayload)

    dayData = fhrPayload.get('data', {}).get('days', {})


    sortedDayInfo = [(hash(date+orphUtils.dictToSortedJsonish(dayData[date])),
                        {"date":date,"data":dayData[date]})
                        for date in sorted(dayData.keys())
                        if ('org.mozilla.appSessions.previous' in dayData[date].keys())]

    # sortedDayInfo = [(hash(date+orphUtils.dictToSortedJsonish(data)),
    #                     {"date":date,"data":data})
    #                     for date,data in fhrPayload.daily_data()
    #                     if ('org.mozilla.appSessions.previous' in data.keys())]

    # [str(profileCreation)+"_"+date+"_"+str(hash(str(orphUtils.dictToSortedTupList(payload["data"]["days"][date])))) for date in payload["data"]["days"].keys() if ('org.mozilla.appSessions.previous' in payload["data"]["days"][date].keys())]

    if sortedDayInfo:
        nodes = sortedDayInfoToDagInfo(sortedDayInfo)
        g=fhrPayload['geckoAppInfo']
        data = {"geo":fhrPayload['geoCountry'],
                "version":g['platformVersion'],
                "os":g['os'],
                "channel":g['updateChannel'],
                "buildId":g['platformBuildID']}

        # print data
        dayGraphOut = poset2.DayGraph(nodes,data)
        dayGraphOut.addChildTreeWidths()
        dayGraphOut.addWidthOffsets()

        context.write(partId,dayGraphOut)
        context.getCounter("MAPPER", "day chain out").increment(1)
    else:
        context.getCounter("MAPPER", "no days with appSessions").increment(1)
