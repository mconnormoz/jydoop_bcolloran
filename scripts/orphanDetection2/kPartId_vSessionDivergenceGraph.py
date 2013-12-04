import jydoop
import healthreportutils
import json
import datetime

'''
in following commands, UPDATE DATES

make ARGS="scripts/orphanDetection2/docIdsInParts.py ./outData/finalDocIdsInParts ./outData/partsOverlap" hadoop

'''

######## to OUTPUT TO HDFS from RAW HBASE
# def skip_local_output():
    # return True


setupjob = jydoop.setupjob


def output(path, results):
    # just dump tab separated key/vals
    f = open(path, 'w')
    for k, v in results:
        print >>f, str(k)+"\t"+str(v)

def localTextInput(mapper):
    #local feeds a line of text input to the function after cleaning it up
    #just ignore the line key. split
    if jydoop.isJython():
        return mapper
    else:
        def localMapper(lineKey,inputLine,context):
            keyValList = inputLine.split("\t")
            key = keyValList[0]

            val = keyValList[1].rstrip()

            return mapper(key,val,context)
        return localMapper


def counterLocal(context,counterGroup,countername,value):
    if jydoop.isJython():
        context.getCounter(counterGroup, countername).increment(value)
    else:
        pass


class bag(object):
    def __add__(self, other):
        result = self.copy()
        for item, count in other.iteritems():
            result._items[item] = result._items.get(item, 0) + count
        return result
    def __and__(self, other):
        result = bag()
        for item, count in other.iteritems():
            new_count = min(self._items.get(item, 0), count)
            if new_count > 0:
                result._items[item] = new_count
        return result
    def __contains__(self, item):
        return item in self._items
    def __eq__(self, other):
        return self._items == other._items
    def __getitem__(self, item):
        return self._items[item]
    def __iadd__(self, other):
        self._items = self.__add__(other)._items
        return self
    def __iand__(self, other):
        self._items = self.__and__(other)._items
        return self
    def __init__(self, iterable=None):
        self._items = {}
        if iterable is not None:
            for item in iterable:
                self._items[item] = self._items.get(item, 0) + 1
    def __ior__(self, other):
        self._items = self.__or__(other)._items
        return self
    def __isub__(self, other):
        self._items = self.__sub__(other)._items
        return self
    def __iter__(self):
        for item, count in self.iteritems():
            for counter in xrange(count):
                yield item
    def __ixor__(self, other):
        self._items = self.__xor__(other)._items
        return self
    def __len__(self):
        return len(self._items)
    def __ne__(self, other):
        return self._items != other._items
    def __or__(self, other):
        result = self.copy()
        for item, count in other.iteritems():
            result._items[item] = max(result._items.get(item, 0),
count)
        return result
    def __repr__(self):
        result = []
        for item, count in self.iteritems():
            result += [repr(item)+":"+repr(count)]
        return '{%s}' % ', '.join(result)
    def __setitem__(self, item, count):
        if not isinstance(count, int) or count < 0:
            raise ValueError
        if count > 0:
            self._items[item] = count
        elif item in self._items:
            del self._items[item]
    def __sub__(self, other):
        result = bag()
        for item, count in self.iteritems():
            new_count = count - other._items.get(item, 0)
            if new_count > 0:
                result._items[item] = new_count
        return result
    def __xor__(self, other):
        result = self.copy()
        for item, count in other.iteritems():
            new_count = abs(result._items.get(item, 0) - count)
            if new_count > 0:
                result._items[item] = new_count
            elif item in result._item:
                del result._items[item]
        return result
    def add(self, item):
        self._items[item] = self._items.get(item, 0) + 1
    def discard(self, item):
        new_count = self._items.get(item, 0) - 1
        if new_count > 0:
            self._items[item] = new_count
        elif new_count == 0:
            del self._items[item]
    def clear(self):
        self._items = {}
    def copy(self):
        result = bag()
        result._items = self._items.copy()
        return result
    def difference(self, other):
        return self.__sub__(other)
    def difference_update(self, other):
        self._items = self.__sub__(other)._items
    def get(self, item, default=0):
        return self._items.get(item, default)
    def intersection(self, other):
        return self.__and__(other)
    def intersection_update(self, other):
        self.__iand__(other)
    def items(self):
        return self._items.items()
    def iteritems(self):
        return self._items.iteritems()
    def iterkeys(self):
        return self._items.iterkeys()
    def itervalues(self):
        return self._items.itervalues()
    def keys(self):
        return self._items.keys()
    def pop(self):
        item = self._items.keys()[0]
        self._items[item] -= 1
        if self._items[item] == 0:
            del self._items[item]
        return item
    def remove(self, item):
        new_count = self._items[item] - 1
        if new_count > 0:
            self._items[item] = new_count
        else:
            del self._items[item]
    def symmetric_difference(self, other):
        return self.__xor__(other)
    def symmetric_difference_update(self, other):
        self.__ixor__(other)
    def union(self, other):
        return self.__or__(other)
    def update(self, other):
        self.__ior__(other)
    def values(self):
        return self._items.values()






def getSessionsWithNum(fhrPayload):
    """Iterate over all session startup times.

    Is a generator of (day, SessionStartTimes) tuples.
    """
    sessionsDict=dict()
    for day, sessions in fhrPayload.daily_provider_data('org.mozilla.appSessions.previous'):
        sessionsDict['day']=dict()
        for i, main in enumerate(sessions.get('main', [])):
            try:
                fp = sessions['firstPaint'][i]
                sr = sessions['sessionRestored'][i]
            except (KeyError, IndexError):
                continue

            yield day, i, healthreportutils.SessionStartTimes(main=main, first_paint=fp,
                session_restored=sr)




def unixDayToIsoDate(unixDay):
    return datetime.datetime.utcfromtimestamp(86400*unixDay).strftime('%Y-%m-%d')




class sessionGraph(object):
    def __init__(self,fhrPayload):
        self.sessionList = [(day_sess[0],day_sess[1],day_sess[2]) for day_sess in getSessionsWithNum(fhrPayload)]
        
        #add the appSession.current
        currentSess = fhrPayload._o.get('data', {}).get('last', {}).get('org.mozilla.appSessions.current', {})
        if currentSess:
            currentSessDate=unixDayToIsoDate(currentSess["startDay"])
            sessNumOnDay = len([num for date,num,startUp in self.sessionList if date==currentSessDate])
            startupTimes = healthreportutils.SessionStartTimes(main=currentSess["main"], first_paint=currentSess["firstPaint"],
                session_restored=currentSess["sessionRestored"])

            self.sessionList+=[(currentSessDate,sessNumOnDay,startupTimes)]

        self.minDate=min(sess[0] for sess in self.sessionList)
        self.maxDate=max(sess[0] for sess in self.sessionList)

        if len(self.sessionList)==0:
            pass
        elif len(self.sessionList)==1:
            self.sessionChain = {hash(self.sessionList[0]): {"date":self.sessionList[0][0],
                "dailySessNum":self.sessionList[0][1],
                "count":1,
                "childTreeWidth":1,
                "earliestInThread":self.minDate,
                "latestInThread":self.maxDate,
                "prev":bag([None]),
                "next":bag([None])}}

            self.initialSessions = set([hash(self.sessionList[0])])
            self.finalSessions = set([hash(self.sessionList[0])])

        if len(self.sessionList)>1:
            # initialize the session chain dict
            self.initialSessions = set([hash(self.sessionList[0])])
            self.sessionChain = {hash(self.sessionList[0]): {"date":self.sessionList[0][0],
                "dailySessNum":self.sessionList[0][1],
                "count":1,
                "childTreeWidth":None,
                "earliestInThread":self.minDate,
                "latestInThread":self.maxDate,
                "prev":bag([None]),
                "next": bag([hash(self.sessionList[1])]) }}

            for i in range(1, len(self.sessionList)-1):
                self.sessionChain[hash(self.sessionList[i])] = {"date":self.sessionList[i][0],
                "dailySessNum":self.sessionList[i][1],
                "count":1,
                "childTreeWidth":None,
                "earliestInThread":self.minDate,
                "latestInThread":self.maxDate,
                "prev":bag([hash(self.sessionList[i-1])]),
                "next":bag([hash(self.sessionList[i+1])])}
            # finalize the chain
            finalSess=len(self.sessionList)-1

            self.finalSessions = set([hash(self.sessionList[finalSess])])

            self.sessionChain[hash(self.sessionList[finalSess])] = {"date":self.sessionList[finalSess][0],
                "dailySessNum":self.sessionList[finalSess-1][1],
                "count":1,
                "childTreeWidth":1,
                "earliestInThread":self.minDate,
                "latestInThread":self.maxDate,
                "prev":bag([hash(self.sessionList[finalSess-1])]),
                "next":bag([None])}



    def __str__(self):
        return str(self.sessionChain)

    def merge(self,otherSessGraph):

        self.finalSessions = self.finalSessions.union(otherSessGraph.finalSessions)
        self.initialSessions = self.initialSessions.union(otherSessGraph.initialSessions)

        for sessId,sessDict in otherSessGraph.sessionChain.items():

            if sessId in self.sessionChain.keys():
                # if this session id is already in the session graph, add the next/prev bags
                self.sessionChain[sessId]["next"] += sessDict["next"]
                self.sessionChain[sessId]["prev"] += sessDict["prev"]
                self.sessionChain[sessId]["count"] += 1
                self.sessionChain[sessId]["earliestInThread"] = min(self.sessionChain[sessId]["earliestInThread"],sessDict["earliestInThread"])
                self.sessionChain[sessId]["latestInThread"] = max(self.sessionChain[sessId]["latestInThread"],sessDict["latestInThread"])

            else: # if this session id is NOT in the session graph, just insert the session from the otherSessGraph
                self.sessionChain[sessId] = sessDict
                self.sessionChain[sessId]["earliestInThread"] = sessDict["earliestInThread"]
                self.sessionChain[sessId]["latestInThread"] = sessDict["latestInThread"]






    def d3GraphJson(self):
        orderedSessionIds = sorted([(sessId,(sessDict["date"],sessDict["dailySessNum"])) for sessId,sessDict in self.sessionChain.items()], key=lambda sessTup: sessTup[1])

        #traverse nodes backwards, adding descendent graph width

        links = []
        nodes = []
        maxSessionCountPerDate=dict()
        for sessId,sessDict in self.sessionChain.items():
            nodes += [{"group":1,"name":str(sessId),"count":sessDict["count"],"date":sessDict["date"],"latestInThread":sessDict["latestInThread"],"earliestInThread":sessDict["earliestInThread"],"numOut":len( [1 for targetId,count in sessDict["next"].items() if targetId] )}]

            maxSessionCountPerDate[sessDict["date"]]=max(maxSessionCountPerDate.get(sessDict["date"],0),sessDict["dailySessNum"])


        for sessId,sessDict in self.sessionChain.items():
            thisNodeNum = next(i for i,node in enumerate(nodes) if node["name"]==str(sessId))

            # the "if targetId" is required to make sure we drop the "None" nodes
            links += [{"source":thisNodeNum,
                        "target":next(i for i,node in enumerate(nodes) if node["name"]==str(targetId)),
                        "value":count} for targetId,count in sessDict['next'].items() if targetId]

        return json.dumps( {"nodes":nodes, "links":links,"maxSessionCountPerDate":maxSessionCountPerDate,"minDate":self.minDate,"maxDate":self.maxDate} )




    def getChildSessionTree(self,sessId):
        children = [self.getChildSessionTree(childSessId) for childSessId in self.sessionChain[sessId]['next'].keys() if childSessId]
        if (children==[]):
            childrenHeight = 1
        else:
            childrenHeight = sum(child["childrenHeight"] for child in children)

        return {"name":sessId,
                    "children":children,
                    "childrenHeight":childrenHeight}


    def addTreeWidthToSessionChain(self,sessId):
        childrenIds = [self.addTreeWidthToSessionChain(childSessId) for childSessId in self.sessionChain[sessId]['next'].keys() if childSessId]

        # print "childrenIds-----    ",childrenIds
        if (childrenIds==[]):
            childrenHeight = 1
            self.sessionChain[sessId]["childrenHeight"]=1
        else:
            try:
                self.sessionChain[sessId]["childrenHeight"] = sum(self.sessionChain[childId]["childrenHeight"] for childId in childrenIds)
            except:
                print "child sessions+++++++++  ",[self.sessionChain[childId] for childId in childrenIds]
                raise

        return sessId



    def allChildrenAccountedFor(self,sessId):
        if self.sessionChain[sessId]["childTreeWidth"]:
            #if the width has alreasy been updated (has a non-zero, non-None value), then all the children are handled.
            return True
        else:
            flag = True if all(self.sessionChain[childId]["childTreeWidth"] for childId in self.sessionChain[sessId]["next"].keys() if childId) else False
            return flag


    def getChildWidths(self,sessId):
        if not self.sessionChain[sessId]["childTreeWidth"]:
            # in this case, 
            return 1
            raise
        else:
            try:
                return max(1, sum(self.sessionChain[childId]["childTreeWidth"] for childId in self.sessionChain[sessId]["next"].keys() if childId))
            except:
                print  "width list", [self.sessionChain[childId]["childTreeWidth"] for childId in self.sessionChain[sessId]["next"].keys() if childId]
                raise

    def addTreeWidthToSessionChain_2(self,idList):
        init = idList
        #bottomIds is a list of all the sessions with no next session
        while(idList):
            # pop the first session
            sessId = idList.pop(0)
            if self.allChildrenAccountedFor(sessId):
                #if all the children have been dealt with, then the width of this node is the width of the sum of all the children of this node. Set that width.
                # sess.width = sum(child.width for child in session.children)
                self.sessionChain[sessId]["childTreeWidth"] = self.getChildWidths(sessId)
                #then proceed up the chain to this session's ancestor, by putting that session at the START of the queue

                idList= [prevId for prevId in self.sessionChain[sessId]["prev"].keys() if prevId] + idList
                # idList.insert(0,sessId)
            else: #otherwise, put this session in the back of the queue
                if sessId in idList:#first, remove the item if it's already in the queue
                    idList.remove(sessId)
                idList.append(sessId)


    def addThreadYPosToSessionChain(self):
        #starting from the first session in the chain, add the thread offset by going down the tree.
        #at each node, set the offset of the CHILD nodes, then add those child nodes to the queue.

        #to initialize, set the offset of the initial sessions, and then add them to the queue

        initialSessions = sorted([sessId for sessId,sessDict in self.sessionChain.items() if None in sessDict["prev"].keys()], key=lambda id: self.sessionChain[id]["latestInThread"], reverse=True)

        queue=[]
        offsets=0
        # find the offsets of the initialSessions
        for sess in initialSessions:
            self.sessionChain[sess]["yPos"] = offsets #will be 0
            offsets+=self.sessionChain[sess]["childTreeWidth"]
            queue.append(sess) #add THIS sess to the queue, so that its children can be checked in the while loop



        while(queue):
            thisSess = queue.pop(0)
            children = sorted([childId for childId in self.sessionChain[thisSess]["next"].keys() if childId], key=lambda id: self.sessionChain[id]["latestInThread"], reverse=True)

            try:
                offsets=self.sessionChain[thisSess]["yPos"]
            except:
                print thisSess, self.sessionChain[thisSess]
                raise
            for sess in children:
                self.sessionChain[sess]["yPos"] = offsets
                offsets+=self.sessionChain[sess]["childTreeWidth"]
                queue.append(sess)




    def d3TreeJson(self):
        # print [(sessId,sessDict["prev"]) for sessId,sessDict in self.sessionChain.items() if None in sessDict["prev"].keys()]

        finalSessions = [sessId for sessId,sessDict in self.sessionChain.items() if None in sessDict["next"].keys()]

        self.addTreeWidthToSessionChain_2(finalSessions)

        self.addThreadYPosToSessionChain()
        return self.sessionChain



    def d3NodesLinksJson(self):
        finalSessions = [sessId for sessId,sessDict in self.sessionChain.items() if None in sessDict["next"].keys()]
        self.addTreeWidthToSessionChain_2(finalSessions)

        self.addThreadYPosToSessionChain()

        nodes = [{"name":sessHash,"date":sessInfo["date"],"dailySessNum":sessInfo["dailySessNum"], "yPos":sessInfo["yPos"]} for sessHash,sessInfo in self.sessionChain.items()]

        links = [{"source":{"date":sessInfo["date"],
                            "dailySessNum":sessInfo["dailySessNum"],
                            "yPos":sessInfo["yPos"]},
                "target":{"date":self.sessionChain[target]["date"],
                            "dailySessNum":self.sessionChain[target]["dailySessNum"],
                            "yPos":self.sessionChain[target]["yPos"]},
                "weight":weight}
                for sessHash,sessInfo in self.sessionChain.items() for target,weight in sessInfo["next"].items() if target!=None]


        return json.dumps({"nodes":nodes,"links":links})












@localTextInput
@healthreportutils.FHRMapper()
def map(partId,fhrPayload,context):
    context.write(partId,fhrPayload)



def reduce(partId, iterOfFhrPayloads, context):
    firstIter=True
    for fhrPayload in iterOfFhrPayloads:
        if firstIter:
            sessionGraphOut = sessionGraph(fhrPayload)
            firstIter=False
        else:
            # print 1
            sessionGraphOut.merge( sessionGraph(fhrPayload) )

    # print "\n------------------------"
    # print sessionGraphOut
    try:
        context.write(partId,sessionGraphOut.d3NodesLinksJson())
    except:
        pass







