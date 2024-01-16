package ru.pandahouse.eulerdb.graph;

public class Properties {
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
    private Long propId;
    private Long nodeId;
    private String author;
    private String authorEmail;

    private Long parentNodeId;

    public Long getPropId() {
        return propId;
    }

    public void setPropId(Long propId) {
        this.propId = propId;
    }

    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getAuthorEmail() {
        return authorEmail;
    }

    public void setAuthorEmail(String authorEmail) {
        this.authorEmail = authorEmail;
    }

    public Long getParentNodeId() {
        return parentNodeId;
    }

    public void setParentNodeId(Long parentNodeId) {
        this.parentNodeId = parentNodeId;
    }
}
