package ru.pandahouse.eulerdb.graph;

public class Relationship {

    private boolean inUse;
    private int firstNode;
    private int secondNode;

    private int firstPrevRelId;
    private int firstNextRelId;

    private int secondPrevRelId;
    private int secondNextRelId;

    private boolean firstInChain;

    public boolean isInUse() {
        return inUse;
    }

    public Relationship setInUse(boolean inUse) {
        this.inUse = inUse;
        return this;
    }

    public int getFirstNode() {
        return firstNode;
    }

    public Relationship setFirstNode(int firstNode) {
        this.firstNode = firstNode;
        return this;
    }

    public int getSecondNode() {
        return secondNode;
    }

    public Relationship setSecondNode(int secondNode) {
        this.secondNode = secondNode;
        return this;
    }

    public int getFirstPrevRelId() {
        return firstPrevRelId;
    }

    public Relationship setFirstPrevRelId(int firstPrevRelId) {
        this.firstPrevRelId = firstPrevRelId;
        return this;
    }

    public int getFirstNextRelId() {
        return firstNextRelId;
    }

    public Relationship setFirstNextRelId(int firstNextRelId) {
        this.firstNextRelId = firstNextRelId;
        return this;
    }

    public int getSecondPrevRelId() {
        return secondPrevRelId;
    }

    public Relationship setSecondPrevRelId(int secondPrevRelId) {
        this.secondPrevRelId = secondPrevRelId;
        return this;
    }

    public int getSecondNextRelId() {
        return secondNextRelId;
    }

    public Relationship setSecondNextRelId(int secondNextRelId) {
        this.secondNextRelId = secondNextRelId;
        return this;
    }

    public boolean isFirstInChain() {
        return firstInChain;
    }

    public Relationship setFirstInChain(boolean firstInChain) {
        this.firstInChain = firstInChain;
        return this;
    }
}
