package edu.uci.ics.textdb.sandbox.team3lucenecityexample;

public class City {
	public City() {
	}
	
	public City(String id, String name, String country, String description) {
		this.id = id;
		this.name = name;
		this.country = country;
		this.description = description;
	}
	
	private String id;
	
	public String getId() {
		return this.id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	private String name;
	
	public String getName() {
		return this.name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	private String country;
	
	public String getCountry() {
		return this.country;
	}
	
	public void setCountry(String country) {
		this.country =  country;
	}
	
	private String description;

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
	public String toString() {
		return "City "+getId()+": "+getName()+" ("+getCountry()+")";
	}
	
	
}
