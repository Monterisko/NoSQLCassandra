import com.datastax.driver.core.Session;

public class KeyspaceRepository {
    private Session session;

    public KeyspaceRepository(Session s){
        session = s;
    }
    public void createKeyspace(
            String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();
        session.execute(query);
    }
    public void deleteKeyspace(String keyspaceName) {
        StringBuilder sb =
                new StringBuilder("DROP KEYSPACE ").append(keyspaceName);

        String query = sb.toString();
        session.execute(query);
    }
}
