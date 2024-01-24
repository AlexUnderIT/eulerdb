package ru.pandahouse.eulerdb.graph;

import java.io.Serializable;

public class Metadata implements Serializable {
    /*
    * {
    * "id": "1c26e2995e72287951ef75ce76f93991ed157e69",
    * "shortId": "1c26e2995",
    * "refs": "",
    * "authorId": "max",
    * authorEmail: "dbi471@gmail.com",
    * title: "конфиг папки добавил, а то потерялись",
    * parents: "41276e25e2803a6b2d7a634100e590eb2f2f5307"
    * }
    */
    private Long nodeId;
    private String authorId;
    private String authorEmail;

    private Long parentNodeId;
    private String title;

    public Metadata setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getTitle() {
        return title;
    }

    public Metadata setAuthorId(String authorId) {
        this.authorId = authorId;
        return this;
    }

    public String getAuthorId() {
        return authorId;
    }

    public Metadata setNodeId(Long nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public Long getNodeId() {
        return nodeId;
    }
    public Metadata setAuthorEmail(String authorEmail) {
        this.authorEmail = authorEmail;
        return this;
    }

    public String getAuthorEmail() {
        return authorEmail;
    }

    public Metadata setParentNodeId(Long parentNodeId) {
        this.parentNodeId = parentNodeId;
        return this;
    }

    public Long getParentNodeId() {
        return parentNodeId;
    }

    @Override
    public String toString() {
        return "Metadata: [" +
                "nodeId = " + nodeId +
                ", authorId = '" + authorId + '\'' +
                ", authorEmail = '" + authorEmail + '\'' +
                ", parentNodeId = " + parentNodeId +
                ", title = '" + title + '\'' +
                ']';
    }
}
