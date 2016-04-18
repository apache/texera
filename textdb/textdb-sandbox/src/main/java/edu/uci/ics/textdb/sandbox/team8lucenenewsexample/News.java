package edu.uci.ics.textdb.sandbox.team8lucenenewsexample;

/**
 * Created by Sam on 16/4/10.
 */
public class News {

    private String id;
    private String title;
    private String category;
    private String text;

    public News() {
    }

    public News(String id, String title, String category, String text) {
        this.id=id;
        this.title=title;
        this.category=category;
        this.text=text;
    }

    public String getId(){
        return this.id;
    }

    public String getTitle(){
        return this.title;
    }

    public String getCategory(){
        return this.category;
    }

    public String getText(){
        return this.text;
    }
}
