package edu.uci.ics.textdb.lucenedemo;


/**
 *
 * @author Rajesh
 */
public class Hotel {
    
    /** Creates a new instance of Accommodation */
    public Hotel() {
    }

    /** Creates a new instance of Accommodation */
    public Hotel(String id, 
                 String name, 
                 String city, 
                 String description) {
        this.id = id;     
        this.name = name;     
        this.description = description;     
        this.city = city;     
    }
    
    /**
     * Holds value of property name.
     */
    private String name;

    /**
     * Getter for property title.
     * @return Value of property title.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Setter for property title.
     * @param title New value of property title.
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
     * @return Value of property id.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Setter for property id.
     * @param id New value of property id.
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
     * @return Value of property details.
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Setter for property details.
     * @param details New value of property details.
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Holds value of property city.
     */
    private String city;

    /**
     * Getter for property city.
     * @return Value of property city.
     */
    public String getCity() {
        return this.city;
    }

    /**
     * Setter for property city.
     * @param city New value of property city.
     */
    public void setCity(String city) {
        this.city = city;
    }

    public String toString() {
        return "Hotel "
               + getId()
               +": "
               + getName()
               +" ("
               + getCity()
               +")";
    }
}
