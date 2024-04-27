import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class Main {
    private Session session;
    private BookRepository bookRepository;

    public static void main(String[] args) {

    }
    @Before
    public void connect() {
        ConnectorCassandra cassandra = new ConnectorCassandra();
        cassandra.connect("127.0.0.1", 32774);
        this.session = cassandra.getSession();
        bookRepository = new BookRepository(session);
    }
    @Test
    public void whenCreatingAKeyspace_thenCreated() {
        KeyspaceRepository schemaRepository;
        schemaRepository = new KeyspaceRepository(session);
        String keyspaceName = "test";
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);

        ResultSet result =
                session.execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = result.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);
        assertEquals(matchedKeyspaces.get(0), keyspaceName.toLowerCase());
    }
    @Test
    public void whenCreatingATable_thenCreatedCorrectly() {
        String KEYSPACE_NAME = "test";
        String BOOKS = "books";
        bookRepository.createTable();
        ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + BOOKS + ";");

        // Collect all the column names in one list.
        List columnNames = result.getColumnDefinitions().asList().stream().map(ColumnDefinitions.Definition::getName).collect(Collectors.toList());
        assertEquals(columnNames.size(), 3);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("title"));
        assertTrue(columnNames.contains("subject"));
    }
    @Test
    public void whenAlteringTable_thenAddedColumnExists() {
        String KEYSPACE_NAME = "test";
        String BOOKS = "books";
        bookRepository.createTable();
        bookRepository.alterTablebooks("publisher", "text");

        ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + BOOKS + ";");

        boolean columnExists = result.getColumnDefinitions().asList().stream().anyMatch(cl -> cl.getName().equals("publisher"));
        assertTrue(columnExists);
    }
    @Test
    public void whenAddingANewBook_thenBookExists() {
        String title = "Effective Java";
        String author = "Joshua Bloch";
        Book book = new Book(UUIDs.timeBased(), title, author, "Programming");
        bookRepository.insertbookByTitle(book);

        Book savedBook = bookRepository.selectByTitle(title);
        assertEquals(book.getTitle(), savedBook.getTitle());
    }
    @Test
    public void whenSelectingAll_thenReturnAllRecords() {

        Book book = new Book(UUIDs.timeBased(), "Effective Java", "Joshua Bloch", "Programming");
        //bookRepository.insertbook(book);

        book = new Book(UUIDs.timeBased(), "Clean Code", "Robert C. Martin", "Programming");
        //bookRepository.insertbook(book);

        List<Book> books = bookRepository.selectAll();
        System.out.println(books);
        assertEquals(2, books.size());
        assertTrue(books.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));
        assertTrue(books.stream().anyMatch(b -> b.getTitle().equals("Clean Code")));
    }
    @Test(expected = InvalidQueryException.class)
    public void whenDeletingATable_thenUnconfiguredTable() {
        bookRepository.deleteTable();

        session.execute("SELECT * FROM " + bookRepository.KEYSPACE_NAME + ".books");
    }
    @Test
    public void whenDeletingAKeyspace_thenDoesNotExist() {
        String keyspaceName = "test";
        KeyspaceRepository schemaRepository = new KeyspaceRepository(session);
        schemaRepository.deleteKeyspace(keyspaceName);

        ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");
        boolean isKeyspaceCreated = result.all().stream().anyMatch(r -> r.getString(0).equals(keyspaceName.toLowerCase()));
        assertFalse(isKeyspaceCreated);
    }
}
