import json


class Poset(object):
    """
    idea: insert a chain of ordered nodes into a graph.
    each node should know which other nodes are above/below it
    """
    def __init__(self,nodeList):
        nodeChain = self._nodeListToChain(nodeList)
        if not nodeChain[0].parentIds:
            #nodeChain[0] must not have a parents elt
            self._possibleMinEltIds = set([nodeChain[0].id])
        else:
            raise ValueError
        if not nodeChain[-1].childIds:
            self._possibleMaxEltIds = set([nodeChain[-1].id])
        else:
            raise ValueError

        self.nodeDict=dict()
        self._minElts = None
        self._maxElts = None
        self._edges=None

        for node in nodeChain:
            self.addNode(node)
    # def __repr__(self):
    #     return json.dumps({"nodes":[node for node in self.nodeDict.values()]},cls=MyEncoder)

    def maxElts(self):
        if not self._maxElts:
            self._maxElts = [node for node in self.nodeDict.values() if not node.children()]
        return self._maxElts

    def minElts(self):
        if not self._minElts:
            self._minElts =[node for node in self.nodeDict.values() if not node.parents()]
        return self._minElts

    def graphWidth(self):
        return self.minElts()[0].childTreeWidth


    def _nodeListToChain(self,nodeList):
        if len(nodeList)==0:
            return nodeList
        elif len(nodeList)==1:
            return nodeList
        else:
            for i in range(len(nodeList)):
                if i==0:
                    nodeList[i].childIds.add(nodeList[i+1].id)
                elif i==len(nodeList)-1:
                    nodeList[i].parentIds.add(nodeList[i-1].id)
                else:
                    nodeList[i].childIds.add(nodeList[i+1].id)
                    nodeList[i].parentIds.add(nodeList[i-1].id)
            return nodeList

    def nodes(self):
        return self.nodeDict.values()

    def addNode(self,node):
        node.containingGraph = self
        if node.id in self.nodeDict:
            self.nodeDict[node.id].mergeNode(node)
        else:
            self.nodeDict[node.id] = node

    def merge(self,otherGraph):
        for node in otherGraph.nodes():
            self.addNode(node)
        # if a graph is merged, min/max elts and edges will need to be re-created
        self._minElts=None
        self._maxElts=None
        self._edges=None

    def edges(self):
        if not self._edges:
            self._edges=[]
            for source in self.nodes():
                self._edges += [{"sourceId":source.id,"targetId":target.id,"count":target.nodeCount} for target in source.children()]
        return self._edges

    def addChildTreeWidths(self):
        queue=[]
        for node in self.maxElts():
            node.childTreeWidth=1
            queue += list(node.parents())

        while queue:
            thisNode = queue.pop(0)
            #remove any dupes of thisNode from the queue
            queue = [node for node in queue if node!=thisNode]

            # if all the child nodes have been handled, then node.childTreeWidth will be a number for all of them 
            thisTreeWidth=0
            for node in thisNode.children():
                if node.childTreeWidth:
                    thisTreeWidth+=node.childTreeWidth
                else:
                    thisTreeWidth = None
                    break
            # try:
            #     thisTreeWidth = sum( node.childTreeWidth for node in thisNode.children() )
            # except:
            #     thisTreeWidth = None

            if thisTreeWidth:
                # this node's can be updated, and its parent can be put on the front of the queue
                thisNode.childTreeWidth = thisTreeWidth
                queue.extend(thisNode.parents())
            else:
                #otherwise, put this node in the back of the queue;
                queue.append(thisNode)

    def addWidthOffsets(self):
        #at each node, set the offset of the CHILD nodes, then add those child nodes to the queue.

        #traverse the graph starting with the minElts; throw an error if there is more than 1
        if len(self.minElts())!=1:
            raise TooManyMinEltsError
        else:
            offset=0
            queue=self.minElts()
            queue[0].widthOffset=offset
            while(queue):
                thisNode = queue.pop(0)
                offset = thisNode.widthOffset
                for childNode in sorted(thisNode.children(), key=lambda node: node.maxDateInThread,reverse=True):
                    childNode.widthOffset = offset
                    offset+=childNode.childTreeWidth
                    queue.append(childNode)








class TooManyMinEltsError(Exception):
    pass



class MyEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__json__()





class Node(object):
    """
    """
    def __init__(self,id,data=None):
        self.id=id
        self.containingGraph=None
        self.childTreeWidth = None
        self.widthOffset = None
        self.nodeCount = 1
        self.data = data
        # self.parents = set() #starts empty, evals to false
        # self.children = set() #starts empty, evals to false
        self.childIds = set() #starts empty, evals to false
        self.parentIds = set() #starts empty, evals to false
    def __str__(self):
        return str({"id":self.id,"children":[node.id for node in self.children()],"parents":[node.id for node in self.parents()]})
    def __repr__(self):
        return str({"id":self.id,"children":[node.id for node in self.children()],"parents":[node.id for node in self.parents()]})
    def __json__(self):
        return {"id":self.id,"children":[node.id for node in self.children()],"parents":[node.id for node in self.parents()]}
    def __hash__(self):
        return self.id
    def __eq__(self,other):
        return self.id==other.id
    def mergeNode(self,otherNode):
        # self.parents.update(otherNode.parents)
        # self.children.update(otherNode.children)
        self.childIds.update(otherNode.childIds)
        self.parentIds.update(otherNode.parentIds)
        self.nodeCount += otherNode.nodeCount
    def children(self):
        if self.childIds:
            return (self.containingGraph.nodeDict[childId] for childId in self.childIds)
        else:
            return []
    def parents(self):
        if self.parentIds:
            return (self.containingGraph.nodeDict[parentId] for parentId in self.parentIds)
        else:
            return []



class DayNode(Node):
    def __init__(self,id,date,minDateInThread,maxDateInThread,data):
        Node.__init__(self,id)
        self.date = date
        # self.widthOffset = None
        # self.nodeCount = 1
        self.data = data
        self.maxDateInThread = maxDateInThread
        self.minDateInThread = minDateInThread
    def __str__(self):
        return json.dumps({"id":self.id,"date":self.date,"childWidth":self.childTreeWidth,"widthOffset":self.widthOffset,"data":self.data},cls=MyEncoder)
    def __repr__(self):
        return json.dumps({"id":self.id,"date":self.date,"nodeCount":self.nodeCount,"childWidth":self.childTreeWidth,"widthOffset":self.widthOffset,"maxDateInThread":self.maxDateInThread,"children":[node.id for node in self.children()],"parents":[node.id for node in self.parents()]})
    def __json__(self):
        return {"id":self.id,"date":self.date,"width":self.nodeCount,"yOffset":self.widthOffset,"data":self.data}
        # ,"childIds":self.childIds,"parentIds":self.parentIds})
    def mergeNode(self,other):
        Node.mergeNode(self,other)
        if self.date!=other.date:
            raise TypeError
        if self.data!=other.data:
            raise TypeError
        self.maxDateInThread = max(self.maxDateInThread,other.maxDateInThread)
        self.minDateInThread = min(self.minDateInThread,other.minDateInThread)



class DayGraph(Poset):
    """docstring for DayGraph"""
    def __init__(self,dayNodeList):
        Poset.__init__(self,dayNodeList)
    def __json__(self):
        return {"minDate":self.minDate(),"maxDate":self.maxDate(),"nodes":self.nodes(),"edges":self.edges(),"graphWidth":self.graphWidth(),"finalThreads":len(self.maxElts())}

    def __repr__(self):
        return json.dumps(self.__json__(),cls=MyEncoder)

    def __str__(self):
        return json.dumps(self.__json__(),cls=MyEncoder)

    def minDate(self):
        return min( node.date for node in self.minElts())

    def maxDate(self):
        return max( node.date for node in self.maxElts())










if __name__=="__main__":

    nodesA = [Node(id=1),Node(id=2),Node(id=3),Node(id=4),Node(id=5)]
    
    posetA=Poset(nodesA)
    for node in posetA.nodeDict.items():
        print node

    dayNodesA = [DayNode(id=1,date="2014-01-01",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=2,date="2014-01-02",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=3,date="2014-01-03",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=4,date="2014-01-04",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=5,date="2014-01-05",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo")
    ]
    
    dayNodesB = [DayNode(id=1,date="2014-01-01",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=2,date="2014-01-02",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=6,date="2014-01-04",minDateInThread="2014-01-01",maxDateInThread="2014-01-04",data="foo")
    ]

    dayNodesC = [
    DayNode(id=2,date="2014-01-02",minDateInThread="2014-01-02",maxDateInThread="2014-01-07",data="foo"),
    DayNode(id=3,date="2014-01-03",minDateInThread="2014-01-02",maxDateInThread="2014-01-07",data="foo"),
    DayNode(id=7,date="2014-01-07",minDateInThread="2014-01-02",maxDateInThread="2014-01-07",data="foo")
    ]

    dayNodesD = [DayNode(id=1,date="2014-01-01",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=2,date="2014-01-02",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=3,date="2014-01-03",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo")]

    dayNodesE = [DayNode(id=1,date="2014-01-01",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=2,date="2014-01-02",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo"),
    DayNode(id=3,date="2014-01-03",minDateInThread="2014-01-01",maxDateInThread="2014-01-05",data="foo")]

    dayPosetA=Poset(dayNodesA)
    for node in dayPosetA.nodeDict.items():
        print node

    dayPosetB=Poset(dayNodesB)
    dayPosetA.merge(dayPosetB)
    for node in dayPosetA.nodeDict.items():
        print node

    dayPosetC=Poset(dayNodesC)
    dayPosetA.merge(dayPosetC)
    dayPosetA.merge(Poset(dayNodesD))
    dayPosetA.merge(Poset(dayNodesE))

    print "minElts:",dayPosetA.minElts(),"\n"
    print "maxElts:",dayPosetA.maxElts(),"\n"
    for node in dayPosetA.nodeDict.items():
        print node

    dayPosetA.addChildTreeWidths()
    dayPosetA.addWidthOffsets()
    print "------------dayNodesA-------------"    
    for node in dayPosetA.nodeDict.items():
        print node


