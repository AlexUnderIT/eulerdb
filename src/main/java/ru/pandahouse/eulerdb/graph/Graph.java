package ru.pandahouse.eulerdb.graph;

import java.util.List;

public class Graph {
    private Long graphId;
    private List<Node> nodeList;
    private List<Relationship> edgeList;

    public Long getGraphId() {
        return graphId;
    }

    public Graph setGraphId(Long graphId) {
        this.graphId = graphId;
        return this;
    }

    public List<Node> getNodeList() {
        return nodeList;
    }

    public Graph setNodeList(List<Node> nodeList) {
        this.nodeList = nodeList;
        return this;
    }

    public List<Relationship> getEdgeList() {
        return edgeList;
    }

    public Graph setEdgeList(List<Relationship> edgeList) {
        this.edgeList = edgeList;
        return this;
    }
}
