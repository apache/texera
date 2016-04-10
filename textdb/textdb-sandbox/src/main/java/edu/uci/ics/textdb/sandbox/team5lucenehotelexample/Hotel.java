package edu.uci.ics.textdb.sandbox.team5lucenemovieexample;

/**
 *
 * @author Parag,Varun
 */
public class Movie {

    /** Creates a new instance of Accommodation */
    public Movie() {
    }

    /** Creates a new instance of Accommodation */
    public Movie(String id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
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
     * Getter for property details.
     * 
     * @return Value of property details.
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Setter for property details.
     * 
     * @param details
     *            New value of property details.
     */
    public void setDescription(String description) {
        this.description = description;
    }

        @Override
    public String toString() {
        return "Movie " + getId() + ": " + getName();
    }
}
