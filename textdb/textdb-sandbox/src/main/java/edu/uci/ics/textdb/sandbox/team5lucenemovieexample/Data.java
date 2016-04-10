package edu.uci.ics.textdb.sandbox.team5lucenemovieexample;

public class Data {

    private static final Movie[] MOVIES = {new Movie("2979220","Mandela: Resistance","History never forgets its heroes, its legends and its victims. During the 20th century there is "),
    		new Movie("2979221","Mandela: The Death of an Icon","A South African news room braces itself to cover the biggest story of its existenceenic criminal to interpret "),
    		new Movie("2979226","Mandella","Hes mad fer it. Fun fact you might not know about "),
    		new Movie("2979259","Manderlay","After gangster Mulligan's cars colony, fleeing northern justice, finds a hiding place in Alabama, spoiled, naves in their cotton fields and following predetermined despicable rules called 'Mam's Law"),
    		new Movie("2979267","Mandie and the Cherokee Treasure","When Uncle John forbids Mandie (Lexi Johnson) from joining his dangerous quest to kl conspirators and dare to face "),
    		new Movie("2979268","Mandie and the Forgotten Christmas","December, 1900 Miss Heathwood's Boarding School for Girls. Thrust into the chaotic anChristmas. This "),
    		new Movie("2979274","Mandingo","Slave owner Warren Maxwell insists that his son, Hammond, who is busy bedding the slaves he buys, marry a white woman and father him a son. While in New Orleans, he picks up a wife, Blanche, a 'bed wench"),
    		new Movie("2979288","Mandingo in a Box","Mandingo in a Box takes an unorthodox look at romance and the black woman's quest for the ever"),
    		new Movie("2979353","Mandorla","It tells the story of Marcos, a young man who, after being left by his girlfriend, discovers his very true rom him and starts to behave differently creating an uncanny moment "),
    		new Movie("2979374","Mandrake, the Magician","Columbia's 7th serial (between Flying G-Men and Overland With Kit Carson)was based on the King F, after 11 chapters, finally catches up to 'The Wasp' in chapter 12, 'The Reward of Trachery', and discovers the villain is "),
    		new Movie("2979375","Mandrake: A Magical Life","Mentalist. Illusionist. Escape Artist. Mandrake was not one magician but many. Who was the real Leon Mandrake? This biography draws back the curtain once more for the great magician who mesmerized North American audiences for over 60 years. The embodiment of his comic book double"),
    		new Movie("2979382","Mandroid","In his hidden laboratory deep in Russia, Dr. Karl Zimmer has invented the Mandroid, a humanoid robot which follrom the CIA for inspection. However Zimmer's partner Drago has different plans, wants to sell Mandroid to the military"),
    		new Movie("2979387","Mandy","Mandy was born deaf and has been mute for all of her life. Her parents believe she is able to speak if she can only be taught and so enrol her with a special teacher. Born deaf, and in time becoming dumb, this is the life of Mandy, a child whose parents have mixed feelings as how to maintain her future, that is until mother and daughter meet the ever-enthusiastic Searle. Enrolling to his school for the deaf, with methods of lip-reading and with the use of objects "),
    		new Movie("2979390","Mandy 2001","Mandy is a story about a young girl... The script is written by and stars thirteen year-old Phoebe Gilpin and offers a truthful perspective on growing up in today'")    };

    public static Movie[] getMovies() {
        return MOVIES;
    }

    public static Movie getMovie(String id) {
        for (Movie movie : MOVIES) {
            if (id.equals(movie.getId())) {
                return movie;
            }
        }
        return null;
    }
}
