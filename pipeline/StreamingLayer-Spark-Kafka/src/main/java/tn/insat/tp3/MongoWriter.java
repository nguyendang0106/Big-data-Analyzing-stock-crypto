package tn.insat.tp3;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.bson.Document;

public class MongoWriter extends ForeachWriter<Row> {

    private final String mongoUri;
    private final String database;
    private final String collectionName;

    private transient MongoClient client;
    private transient MongoCollection<Document> collection;

    public MongoWriter(String mongoUri, String database, String collectionName) {
        this.mongoUri = mongoUri;
        this.database = database;
        this.collectionName = collectionName;
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        try {
            client = MongoClients.create(mongoUri);
            collection = client.getDatabase(database).getCollection(collectionName);
            return true;
        } catch (Exception e) {
            System.err.println("MongoWriter OPEN error: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void process(Row row) {
        try {
            Document doc = Document.parse(row.json());
            collection.insertOne(doc);
        } catch (Exception e) {
            System.err.println("MongoWriter INSERT error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        if (errorOrNull != null) {
            System.err.println("MongoWriter CLOSE error: " + errorOrNull.getMessage());
            errorOrNull.printStackTrace();
        }
        if (client != null) {
            client.close();
        }
    }
}
