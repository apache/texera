package edu.uci.ics.textdb.sandbox.team4lucenebooksexample;

public class Data {

    private static final Book[] BOOKS = {
            new Book(
                    "1",
                    "The Jungle Book",
                    "Rudyard Kipling",
                    "The Jungle Book (1894) is a collection of stories by English author Rudyard Kipling. The stories were first published in magazines in 1893–94. The original publications contain illustrations, some by Rudyard's father, John Lockwood Kipling."),
            new Book(
                    "2",
                    " Harry Potter and the Cursed Child",
                    "J.K. Rowling",
                    " Based on an original new story by J.K. Rowling, Jack Thorne and John Tiffany, a new play by Jack Thorne, Harry Potter and the Cursed Child is the eighth story in the Harry Potter series and the first official Harry Potter story to be presented on stage."),
            new Book(
                    "3",
                    "Brave Enough",
                    "Cheryl Strayed ",
                    "From the best-selling author of Wild, a collection of quotes--drawn from the wide range of her writings--that capture her wisdom, courage, and outspoken humor, presented in a gift-sized package that's as irresistible to give as it is to receive."),
            new Book(
                    "4",
                    "Hamilton: The Revolution",
                    "Lin-Manuel Miranda",
                    "Lin-Manuel Miranda's groundbreaking musical Hamilton is as revolutionary as its subject, the poor kid from the Caribbean who fought the British, defended the Constitution, and helped to found the United States."),
            new Book(
                    "5",
                    "The Rainbow Comes and Goes",
                    "Anderson Cooper",
                    "A touching and intimate correspondence between Anderson Cooper and his mother, Gloria Vanderbilt, offering timeless wisdom and a revealing glimpse into their lives."),

    };

    public static Book[] getBooks() {
        return BOOKS;
    }

    public static Book getBook(String id) {
        for (Book book : BOOKS) {
            if (id.equals(book.getId())) {
                return book;
            }
        }
        return null;
    }
}
