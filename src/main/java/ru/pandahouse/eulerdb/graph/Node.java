package ru.pandahouse.eulerdb.graph;

// 9 байт
public class Node {

    private boolean inUse;
    private int nextRelId;
    private int nextPropId;

    public boolean isInUse() {
        return inUse;
    }

    public Node setInUse(boolean inUse) {
        this.inUse = inUse;
        return this;
    }

    public int getNextRelId() {
        return nextRelId;
    }

    public Node setNextRelId(int nextRelId) {
        this.nextRelId = nextRelId;
        return this;
    }

    public int getNextPropId() {
        return nextPropId;
    }

    public Node setNextPropId(int nextPropId) {
        this.nextPropId = nextPropId;
        return this;
    }
}
