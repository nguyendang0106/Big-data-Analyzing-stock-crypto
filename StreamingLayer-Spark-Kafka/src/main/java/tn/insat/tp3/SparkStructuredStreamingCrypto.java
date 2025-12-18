        package tn.insat.tp3;

        import io.github.cdimascio.dotenv.Dotenv;
        import com.mongodb.client.MongoClient;
        import com.mongodb.client.MongoClients;
        import com.mongodb.client.MongoCollection;
        import org.apache.spark.api.java.function.VoidFunction2;
        import org.apache.spark.sql.*;
        import org.apache.spark.sql.streaming.StreamingQuery;
        import org.apache.spark.sql.streaming.Trigger;
        import org.apache.spark.sql.types.DataTypes;
        import org.apache.spark.sql.types.StructType;
        import org.bson.Document;

        import java.util.ArrayList;
        import java.util.Iterator;
        import java.util.List;

        import static org.apache.spark.sql.functions.*;

        public class SparkStructuredStreamingCrypto {

        private static final int MONGO_BATCH_SIZE = 1000;

        public static void main(String[] args) throws Exception {

                
                Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
                String KAFKA_TOPIC = "binance";
                String mongoUri = dotenv.get("MONGO_URI");
                String mongoDb = dotenv.get("MONGO_DB");
                String mongoRawCollection = dotenv.get("MONGO_RAW_COLLECTION");
                String mongoTumblingCollection = dotenv.get("MONGO_TUMBLING_COLLECTION");
                String mongoSlidingCollection = dotenv.get("MONGO_SLIDING_COLLECTION");
                

                if (mongoUri == null || mongoDb == null) {
                        throw new RuntimeException("MongoDB environment variables are not set");
                }

                testMongoConnection(mongoUri, mongoDb);

                SparkSession spark = SparkSession
                        .builder()
                        .appName("SparkCryptoStructuredStreaming")
                        .master("local[*]")
                        .config("spark.sql.caseSensitive", "true")
                        .getOrCreate();

                spark.sparkContext().setLogLevel("WARN");

                // ================================
                // SCHEMA FOR RAW KAFKA DATA
                // ================================
                StructType dataSchema = new StructType()
                        .add("e", DataTypes.StringType)
                        .add("E", DataTypes.LongType)
                        .add("s", DataTypes.StringType)
                        .add("t", DataTypes.LongType)
                        .add("p", DataTypes.StringType)
                        .add("q", DataTypes.StringType)
                        .add("T", DataTypes.LongType)
                        .add("m", DataTypes.BooleanType)
                        .add("M", DataTypes.BooleanType);

                StructType binanceSchema = new StructType()
                        .add("stream", DataTypes.StringType)
                        .add("data", dataSchema);

                // ================================
                // READ KAFKA
                // ================================
                Dataset<Row> kafkaDF = spark.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", "localhost:9092")
                        .option("subscribe", KAFKA_TOPIC)
                        .option("startingOffsets", "latest")
                        .load();

                Dataset<Row> parsedDF = kafkaDF
                        .selectExpr("CAST(value AS STRING) AS json")
                        .select(from_json(col("json"), binanceSchema).as("r"))
                        .select("r.*");

                Dataset<Row> tradeDF = parsedDF
                        .select(
                                col("stream"),
                                col("data.e").alias("event_type"),
                                col("data.E").alias("event_time"),
                                col("data.s").alias("symbol"),
                                col("data.t").alias("trade_id"),
                                col("data.p").alias("price_str"),
                                col("data.q").alias("quantity_str"),
                                col("data.T").alias("trade_time"),
                                col("data.m").alias("is_buyer_maker")
                        )
                        .withColumn("price", col("price_str").cast(DataTypes.DoubleType))
                        .withColumn("quantity", col("quantity_str").cast(DataTypes.DoubleType))
                        .withColumn("timestamp", to_timestamp(col("trade_time").divide(1000)));

                // ============================================================
                // RAW → GHI THẲNG VÀO raw_trade
                // ============================================================
                VoidFunction2<Dataset<Row>, Long> rawBatchWriter = (df, batchId) -> {
                try {
                        if (df == null || df.isEmpty()) return;
                        writeDatasetToMongoBatch(df, mongoUri, mongoDb, mongoRawCollection);
                } catch (Exception e) {
                        System.err.println(" Error writing RAW batch: " + e.getMessage());
                        e.printStackTrace();
                }
                };

                StreamingQuery rawQuery = tradeDF
                        .writeStream()
                        .foreachBatch(rawBatchWriter)
                        .outputMode("append")
                        .option("checkpointLocation", "checkpoint/raw")
                        .start();

                // ============================================================
                // TUMBLING 1-MINUTE WINDOW
                // ============================================================
                Dataset<Row> tumblingAgg = tradeDF
                        .withWatermark("timestamp", "5 seconds")
                        .groupBy(
                                window(col("timestamp"), "1 minute"),
                                col("symbol")
                        )
                        .agg(
                                first("price").alias("open"),
                                max("price").alias("high"),
                                min("price").alias("low"),
                                last("price").alias("close"),
                                sum("quantity").alias("volume"),
                                count("*").alias("trade_count")
                        )
                        .select(
                                col("symbol"),
                                col("window.start").alias("start_time"),
                                col("window.end").alias("end_time"),
                                col("open"),
                                col("high"),
                                col("low"),
                                col("close"),
                                col("volume"),
                                col("trade_count")
                        );

                VoidFunction2<Dataset<Row>, Long> tumblingBatchWriter = (df, batchId) -> {
                try {
                        if (df == null || df.isEmpty()) return;
                        writeDatasetToMongoBatch(df, mongoUri, mongoDb, mongoTumblingCollection);
                } catch (Exception e) {
                        System.err.println(" Error writing TUMBLING batch: " + e.getMessage());
                        e.printStackTrace();
                }
                };

                StreamingQuery tumblingQuery = tumblingAgg
                        .writeStream()
                        .foreachBatch(tumblingBatchWriter)
                        .outputMode("update")
                        .option("checkpointLocation", "checkpoint/tumbling")
                        .start();

                // ============================================================
                // SLIDING WINDOW 30s SLIDE / 1min WINDOW
                // ============================================================
                Dataset<Row> slidingAgg = tradeDF
                        .withWatermark("timestamp", "5 seconds")
                        .groupBy(
                                window(col("timestamp"), "1 minute", "30 seconds"),
                                col("symbol")
                        )
                        .agg(
                                first("price").alias("open"),
                                max("price").alias("high"),
                                min("price").alias("low"),
                                last("price").alias("close"),
                                sum("quantity").alias("volume"),
                                count("*").alias("trade_count")
                        )
                        .select(
                                col("symbol"),
                                col("window.start").alias("start_time"),
                                col("window.end").alias("end_time"),
                                col("open"),
                                col("high"),
                                col("low"),
                                col("close"),
                                col("volume"),
                                col("trade_count")
                        );

                VoidFunction2<Dataset<Row>, Long> slidingBatchWriter = (df, batchId) -> {
                try {
                        if (df == null || df.isEmpty()) return;
                        writeDatasetToMongoBatch(df, mongoUri, mongoDb, mongoSlidingCollection);
                } catch (Exception e) {
                        System.err.println(" Error writing SLIDING batch: " + e.getMessage());
                        e.printStackTrace();
                }
                };

                StreamingQuery slidingQuery = slidingAgg
                        .writeStream()
                        .foreachBatch(slidingBatchWriter)
                        .outputMode("update")
                        .option("checkpointLocation", "checkpoint/sliding")
                        .start();

                rawQuery.awaitTermination();
                tumblingQuery.awaitTermination();
                slidingQuery.awaitTermination();
        }

        // =====================================================================
        // BATCH INSERT INTO MONGO
        // =====================================================================
        private static void writeDatasetToMongoBatch(Dataset<Row> df, String mongoUri,
                                                        String dbName, String collectionName) {

                MongoClient client = null;

                try {
                client = MongoClients.create(mongoUri);
                MongoCollection<Document> collection =
                        client.getDatabase(dbName).getCollection(collectionName);

                Iterator<String> iter = df.toJSON().toLocalIterator();
                List<Document> buffer = new ArrayList<>(MONGO_BATCH_SIZE);

                while (iter.hasNext()) {
                        buffer.add(Document.parse(iter.next()));

                        if (buffer.size() >= MONGO_BATCH_SIZE) {
                        collection.insertMany(new ArrayList<>(buffer));
                        buffer.clear();
                        }
                }

                if (!buffer.isEmpty()) collection.insertMany(buffer);

                } catch (Exception e) {
                System.err.println(" Mongo batch insert error (" + collectionName + ") → " + e.getMessage());
                e.printStackTrace();
                } finally {
                if (client != null) client.close();
                }
        }

        private static void testMongoConnection(String mongoUri, String dbName) {
                try (MongoClient client = MongoClients.create(mongoUri)) {

                        Document ping = new Document("ping", 1);
                        client.getDatabase(dbName).runCommand(ping);

                        System.out.println(" Successfully connected to MongoDB Atlas");

                } catch (Exception e) {
                        System.err.println(" Failed to connect to MongoDB Atlas");
                        e.printStackTrace();
                        System.exit(1);
                }
                }

        }
