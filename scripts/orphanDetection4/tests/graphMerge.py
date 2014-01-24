
class weightedDag(object):
    """
    nodes are stored in a dict: {nodeLabel:nodeInfo,...}
    edges are stored in a dict: {(sourceLabel,targetLabel):edgeInfo,...}
    """
    def __init__(self,nodes,edges):
        self.nodesDict=nodes
        self.edgesDict=edges
    def __repr__(self):
        print self.nodesDict
        print self.edgesDict
    def __str__(self):
        return "nodes:"+str(self.nodesDict)+", edges:"+str(self.edgesDict)

    def edgeInfo(self,edgeLabel):
        return self.edgesDict[edgeLabel]

    def hasNode(self,nodeLabel):
        return (nodeLabel in self.nodesDict.keys())

    def nodes(self):
        for node in self.nodesDict.items():
            yield node

    def hasEdge(self,edgeLabel):
        return (edgeLabel in self.edgesDict.keys())

    def add_node(self, nodeLabel, nodeInfo=None):
        self.nodesDict[nodeLabel]=nodeInfo

    def add_nodes(self, nodeList):
        for nodeLabel,nodeInfo in nodeList:
            self.add_node(nodeLabel,nodeInfo)

    def add_edge(self, edgeLabel, edgeInfo=None):
        if self.hasEdge(edgeLabel):
            self.edgesDict[edgeLabel]=self.combineEdgeInfo(self.edgeInfo(edgeLabel),edgeInfo)

    def add_edges(self, edgeList):
        for edgeLabel,edgeInfo in edgeList:
            self.add_edge(edgeLabel,edgeInfo)


    def merge_graph(self, other):
        """
        merge other graph with this graph.
              
        @type  other: graph
        @param other: Graph
        """
        self.add_nodes( (nLabel,nInfo) for nLabel,nInfo in other.nodes() )
       
        for nLabel,nInfo in other.nodes():
            for edgeLabel,edgeInfo in other.edgesFrom(nLabel):
                self.add_edge(edgeLabel,edgeInfo)


    def edgesFrom(self,nodeLabel):
        for edgeLabel,edgeInfo in self.edgesDict.items():
            if edgeLabel[0]==nodeLabel:
                yield (edgeLabel,edgeInfo)



class dayDag(weightedDag):
    def combineEdgeInfo(self,edgeInfo1,edgeInfo2):
        return edgeInfo1+edgeInfo2


a=dayDag({1:"a",2:"b",3:42324,4:"f"},{(1,2):1,(2,3):1,(3,4):1})
b=dayDag({1:"a",2:"b",3:42324,4:"f",5:"g",6:"g"},{(1,2):1,(2,3):1,(3,4):1,(4,6):1,(4,5):1})

c=dayDag({1:"a",2:"b",3:42324,4:"f",5:"g",6:"g"},{(1,2):1,(2,3):1,(3,4):1,(4,6):1,(4,5):1})
c.merge_graph(a)
c.merge_graph(b)
print a
print b
print c
