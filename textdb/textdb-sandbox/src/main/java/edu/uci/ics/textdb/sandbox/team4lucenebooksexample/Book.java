package edu.uci.ics.textdb.sandbox.team4lucenebooksexample;

/**
 *
 * @author Akshay, Prakul
 */
public class Book {

    /** Creates a new instance of Book */
    public Book() {
    }

    /** Creates a new instance of Book */
    public Book(String id, String name, String author, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.author = author;
    }

    /**
     * Holds value of property name.
     */
    private String name;

    /**
     * Getter for property title.
     * 
     * @return Value of property title.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Setter for property title.
     * 
     * @param title
     *            New value of property title.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Holds value of property id.
     */
    private String id;

    /**
     * Getter for property id.
     * 
     * @return Value of property id.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Setter for property id.
     * 
     * @param id
     *            New value of property id.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Holds value of property description.
     */
    private String description;

    /**
     * Getter for property description.
     * 
     * @return Value of property description.
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Setter for property description.
     * 
     * @param description
     *            New value of property description.
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Holds value of property author.
     */
    private String author;

    /**
     * Getter for property author.
     * 
     * @return Value of property author.
     */
    public String getAuthor() {
        return this.author;
    }

    /**
     * Setter for property author.
     * 
     * @param author
     *            New value of property author.
     */
    public void setAuthor(String author) {
        this.author = author;
    }

    @Override
    public String toString() {
        return "Books " + getId() + ": " + getName() + " (" + getAuthor() + ")";
    }
}
