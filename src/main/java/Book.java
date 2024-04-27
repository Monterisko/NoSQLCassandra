import java.util.UUID;

public class Book {
    private UUID id;
    private  String title;
    private String author;
    private String subject;

    public Book(UUID id,String title, String author, String subject){
        this.id = id;
        this.title = title;
        this.author = author;
        this.subject = subject;
    }

    public UUID getId(){
        return id;
    }
    public String getTitle(){
        return title;
    }
    public String getAuthor(){
        return author;
    }
    public String getSubject(){
        return subject;
    }

    @Override
    public String toString() {
        return "Book{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", author='" + author + '\'' +
                ", subject='" + subject + '\'' +
                '}';
    }
}
