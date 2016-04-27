package edu.uci.ics.textdb.sandbox.team3.team3lucenecityexample;

public class Data {

    private static final City[] CITIES = {
            new City(
                    "1",
                    "Shanghai",
                    "China",
                    "Shanghai is the largest Chinese city by population and the largest city proper by population in the world. It is one of the four direct-controlled municipalities of China, with a population of more than 24 million as of 2014. It is a global financial center, and a transport hub with the world's busiest container port."),
            new City(
                    "2",
                    "New York City",
                    "The United States",
                    "New York City, or simply New York, is the most populous city in the United States. Located at the southern tip of the State of New York, the city is the center of the New York metropolitan area, one of the most populous urban agglomerations in the world."),
            new City(
                    "3",
                    "San Diego",
                    "The United States",
                    "San Diego is a major city in California, on the coast of the Pacific Ocean in Southern California, approximately 120 miles (190 km) south of Los Angeles and immediately adjacent to the border with Mexico."),
            new City(
                    "4",
                    "Seattle",
                    "The United States",
                    "Seattle is a West Coast seaport city and the seat of King County. With an estimated 662,400 residents as of 2015, Seattle is the largest city in both the state of Washington and the Pacific Northwest region of North America. "),
            new City(
                    "5",
                    "Manila",
                    "Philippines",
                    "Manila is the capital city of the Philippines. It is the home to extensive commerce and seats the executive and judicial branches of the Filipino government. It also contains vast amount of significant architectural and cultural landmarks in the country."),
            new City(
                    "6",
                    "Bangkok",
                    "Thailand",
                    "Bangkok is the capital and most populous city of Thailand. The city occupies 1,568.7 square kilometres (605.7 sq mi) in the Chao Phraya River delta in Central Thailand, and has a population of over 8 million, or 12.6 percent of the country's population."),
            new City(
                    "7",
                    "Mumbai",
                    "India",
                    "Mumbai is the capital city of the Indian state of Maharashtra. It is the most populous city in India and the ninth most populous agglomeration in the world, with an estimated city population of 18.4 million."),
            new City(
                    "8",
                    "Barcelona",
                    "Spain",
                    "Barcelona is the capital city of the autonomous community of Catalonia in Spain and Spain's second most populated city, with a population of 1.6 million within its administrative limits."),
            new City(
                    "9",
                    "Cairo",
                    "Egypt",
                    "Cairo is the capital and largest city of Egypt. The city's metropolitan area is the largest in the Middle East and the Arab world, and 15th-largest in the world, and is associated with ancient Egypt, as the famous Giza pyramid complex and the ancient city of Memphis are located in its geographical area."),
            new City(
                    "10",
                    "Auckland",
                    "New Zealand",
                    "Auckland, in the North Island of New Zealand, is the largest and most populous urban area in the country. Auckland has a population of 1,454,300 - 32 percent of New Zealand's population."),
            new City(
                    "11",
                    "Santiago",
                    "Chile",
                    "Santiago, also known as Santiago de Chile, is the capital and largest city of Chile. It is also the center of its largest conurbation. Santiago is located in the country's central valley, at an elevation of 520 m (1,706 ft) above mean sea level."),

    };

    public static City[] getCities() {
        return CITIES;
    }

    public static City getCity(String id) {
        for (City City : CITIES) {
            if (id.equals(City.getId())) {
                return City;
            }
        }
        return null;
    }
}
