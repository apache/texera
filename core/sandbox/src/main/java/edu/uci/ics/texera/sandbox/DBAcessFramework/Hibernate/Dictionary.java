package edu.uci.ics.texera.dataflow.resource.dictionary;

import javax.persistence.Entity;

@Entity
public class Dictionary {
    private String id;
    private String content;

    public Dictionary() {}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }


    public void setContent(String content) {
        this.content = content;
    }
}