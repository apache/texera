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
    
    
    private String name;

    /**
     * 
     * returns property title.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sets property title.
     * 
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Holds value of property id.
     */
    private String id;

    /**
     * 
     * returns Value of property id.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Sets property id.
     * 
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Holds value of property description.
     */
    private String description;

    /**
     * returns property details.
     * 
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Sets property details.
     * 
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Holds value of property city.
     */
    private String city;

    /**
     *
     * returns Value of city.
     */
    public String getCity() {
        return this.city;
    }

    /**
     * Sets city.
     * 
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
