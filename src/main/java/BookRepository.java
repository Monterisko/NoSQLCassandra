import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

public class BookRepository {
    private static final String TABLE_NAME = "books";
    private Session session;
    String KEYSPACE_NAME = "test";
    public BookRepository(Session s){
        session = s;
    }

    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(KEYSPACE_NAME).append(".")
                .append(TABLE_NAME).append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("title text,")
                .append("subject text);");

        String query = sb.toString();
        session.execute(query);
    }
    public void alterTablebooks(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(KEYSPACE_NAME).append(".")
                .append(TABLE_NAME).append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");

        String query = sb.toString();
        session.execute(query);
    }
    public void insertbookByTitle(Book book) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(KEYSPACE_NAME).append(".")
                .append(TABLE_NAME).append("(id, title) ")
                .append("VALUES (").append(book.getId())
                .append(", '").append(book.getTitle()).append("');");

        String query = sb.toString();
        session.execute(query);
    }
    public List<Book> selectAll() {
        StringBuilder sb =
                new StringBuilder("SELECT * FROM ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME);

        String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Book> books = new ArrayList<Book>();

        rs.forEach(r -> {
            books.add(new Book(
                    r.getUUID("id"),
                    r.getString("title"),
                    r.getString("publisher"),
                    r.getString("subject")));
        });
        return books;
    }
    public Book selectByTitle(String title){
        StringBuilder sb = new StringBuilder(
                "SELECT * FROM ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME).append(" WHERE title='")
                .append(title).append("'").append("ALLOW FILTERING");
        String query = sb.toString();
        ResultSet rs = session.execute(query);
        List<Book> books = new ArrayList<Book>();

        rs.forEach(r -> {
            books.add(new Book(
                    r.getUUID("id"),
                    r.getString("title"),
                    r.getString("publisher"),
                    r.getString("subject")));
        });
        return books.get(0);
    }

    public void deleteByTitle(String title){
        Book books = selectByTitle(title);
        StringBuilder sb = new StringBuilder(
                "DELETE publisher, subject, title FROM").append(KEYSPACE_NAME).append(".").append(TABLE_NAME)
                .append("WHERE id =").append(books.getId());
        String query = sb.toString();
        session.execute(query);
    }

    public void insertbook(Book book) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(KEYSPACE_NAME).append(".")
                .append(TABLE_NAME).append("(id, title, subject, publisher) ")
                .append("VALUES (").append(book.getId())
                .append(", '").append(book.getTitle())
                .append("', '").append(book.getSubject())
                .append("', '").append(book.getAuthor()).append("');");
        String query = sb.toString();
        session.execute(query);
    }
    public void deleteTable() {
        StringBuilder sb =
                new StringBuilder("DROP TABLE IF EXISTS ").append(KEYSPACE_NAME).append(".").append(TABLE_NAME);

        String query = sb.toString();
        session.execute(query);
    }

}
