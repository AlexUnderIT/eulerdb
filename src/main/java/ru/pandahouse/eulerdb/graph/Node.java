package ru.pandahouse.eulerdb.graph;

import java.util.UUID;

// 9 байт
public class Node {

    private UUID id;
    private boolean inUse;
    private int nextRelId;
    private int nextPropId;

    public boolean isInUse() {
        return inUse;
    }

    public UUID getId() {
        return id;
    }

    public Node setId(UUID id) {
        this.id = id;
        return this;
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
