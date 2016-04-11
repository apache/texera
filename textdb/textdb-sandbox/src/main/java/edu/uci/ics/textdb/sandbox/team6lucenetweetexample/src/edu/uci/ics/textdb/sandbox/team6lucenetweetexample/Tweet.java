package edu.uci.ics.textdb.sandbox.team6lucenetweetexample;

/**
 *
 * @author Rajesh
 */
public class Tweet {
    private String id;
    private String date;
    private String user;
    private String text;

    /** Creates a new instance of Tweet using null values */
    public Tweet() {
    }

    /** Creates a new instance of Tweet
     * @param id Tweet ID
     * @param date Tweet creation date
     * @param user Tweet creator
     * @param text Tweet text
     */
    public Tweet(String id, String date, String user, String text) {
        this.id = id;
        this.date = date;
        this.user = user;
        this.text = text;
    }

    /**
     * Getter for property id.
     * 
     * @return Value of property date.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Setter for property date.
     * 
     * @param id
     * Set new value of id.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Getter for property date.
     * 
     * @return Value of property date.
     */
    public String getDate() {
        return this.date;
    }

    /**
     * Setter for property date.
     * 
     * @param date
     * Set new value of date.
     */
    public void setDate(String date) {
        this.date = date;
    }

    /**
     * Getter for property user.
     * 
     * @return Value of property user.
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Setter for property user.
     * 
     * @param user
     * Set new value of user.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Getter for property text.
     * 
     * @return Value of property text.
     */
    public String getText() {
        return this.text;
    }

    /**
     * Setter for property text.
     * 
     * @param text
     * Set new value of text.
     */
    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "Tweet " + getId() + ": " + getUser() + " - " + getDate() + "(" + getText() + ")";
    }
}
