package tn.insat.tp3;


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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.BulkWriteOptions;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkStructuredStreamingCrypto {

    private static final int MONGO_BATCH_SIZE = 1000;

    public static void main(String[] args) throws Exception {


        String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");
        if (KAFKA_TOPIC == null || KAFKA_TOPIC.isEmpty()) {
        throw new RuntimeException("KAFKA_TOPIC is not set");
        }

        String KAFKA_BOOTSTRAP = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (KAFKA_BOOTSTRAP == null || KAFKA_BOOTSTRAP.isEmpty()) {
        throw new RuntimeException("KAFKA_BOOTSTRAP_SERVERS is not set");
        }

        String mongoUri = System.getenv("MONGO_URI");
        String mongoDb = System.getenv("MONGO_DB");
        String mongoTumblingCollection = System.getenv("MONGO_TUMBLING_COLLECTION");
        String mongoSlidingCollection = System.getenv("MONGO_SLIDING_COLLECTION");

        String gcsKeyPath = System.getenv("GCS_KEY_PATH");
        String gcsBucket = System.getenv("GCS_BUCKET");
        String gcsRawPath = System.getenv("GCS_RAW_PATH");
                

        if (mongoUri == null || mongoDb == null) {
            throw new RuntimeException("MongoDB environment variables are not set");
        }

        if (gcsKeyPath == null || gcsBucket == null || gcsRawPath == null) {
            throw new RuntimeException("GCS environment variables are not set");
        }

        testMongoConnection(mongoUri, mongoDb);

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkCryptoStructuredStreaming")
                .master("local[*]")
                .config("spark.sql.caseSensitive", "true")
                .config("spark.sql.shuffle.partitions", "4")
                // GCS Configuration
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcsKeyPath)
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
                // .option("kafka.bootstrap.servers", "localhost:9092")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", KAFKA_TOPIC)
                // .option("startingOffsets", "latest")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", "50000")
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
                .withColumn("timestamp", to_timestamp(col("trade_time").divide(1000)))
                .withColumn("date", to_date(col("timestamp"))) // Thêm cột date để partition
                .withColumn("hour", hour(col("timestamp")));   // Thêm cột hour để partition

        // ============================================================
        // RAW → GHI VÀO GCS (PARQUET FORMAT)
        // ============================================================
        String gcsOutputPath = String.format("gs://%s/%s", gcsBucket, gcsRawPath);
        
        System.out.println(" Writing RAW trades to GCS: " + gcsOutputPath);

        StreamingQuery rawQuery = tradeDF
                .coalesce(1)
                .writeStream()
                .format("parquet")
                .option("path", gcsOutputPath)
                .option("checkpointLocation", "/checkpoint/raw-gcs-v3")
                .partitionBy("date", "hour") // Partition theo ngày và giờ
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("1 minute")) // Ghi mỗi phút
                .start();

        // ============================================================
        // TUMBLING 1-MINUTE WINDOW → MONGODB
        // ============================================================
        Dataset<Row> tumblingAgg = tradeDF
                // .withWatermark("timestamp", "5 seconds")
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
                .option("checkpointLocation", "/checkpoint/tumbling-v2")
                .start();

        // ============================================================
        // SLIDING WINDOW 30s SLIDE / 1min WINDOW → MONGODB
        // ============================================================
        Dataset<Row> slidingAgg = tradeDF
                // .withWatermark("timestamp", "5 seconds")
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
                .option("checkpointLocation", "/checkpoint/sliding-v2")
                .start();

        rawQuery.awaitTermination();
        tumblingQuery.awaitTermination();
        slidingQuery.awaitTermination();
    }

    // =====================================================================
    // BATCH INSERT INTO MONGO
    // =====================================================================
        // private static void writeDatasetToMongoBatch(Dataset<Row> df, String mongoUri, String dbName, String collectionName) {
        // df.foreachPartition(partition -> { // Dùng foreachPartition hiệu quả hơn tạo Client liên tục
        //         if (!partition.hasNext()) return;

        //         try (MongoClient client = MongoClients.create(mongoUri)) {
        //         MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
                
        //         // Tùy chọn Upsert: Tìm thấy thì ghi đè, không thấy thì thêm mới
        //         ReplaceOptions opts = new ReplaceOptions().upsert(true);

        //         while (partition.hasNext()) {
        //                 Row row = partition.next();
        //                 Document doc = Document.parse(row.json());
                        
        //                 // Key để xác định duy nhất một cây nến là: SYMBOL + START_TIME + END_TIME
        //                 // Ví dụ: Tìm bản ghi của BTCUSDT lúc 09:00 - 09:01
        //                 Bson filter = Filters.and(
        //                 Filters.eq("symbol", row.getAs("symbol")),
        //                 Filters.eq("start_time", row.getAs("start_time")),
        //                 Filters.eq("end_time", row.getAs("end_time"))
        //                 );

        //                 collection.replaceOne(filter, doc, opts);
        //         }
        //         } catch (Exception e) {
        //         e.printStackTrace();
        //         }
        // });
        // }
        private static void writeDatasetToMongoBatch(Dataset<Row> df, String mongoUri, String dbName, String collectionName) {
                df.foreachPartition(partition -> {
                if (!partition.hasNext()) return;

                try (MongoClient client = MongoClients.create(mongoUri)) {
                        MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
                        
                        // List chứa các lệnh write để gửi 1 lần
                        List<WriteModel<Document>> bulkOperations = new ArrayList<>();
                        ReplaceOptions upsertOptions = new ReplaceOptions().upsert(true);
                        
                        // Cấu hình BulkWrite: ordered=false giúp chạy song song, nhanh hơn
                        BulkWriteOptions bulkOptions = new BulkWriteOptions().ordered(false);

                        while (partition.hasNext()) {
                        Row row = partition.next();
                        Document doc = Document.parse(row.json());

                        // Tạo filter
                        Bson filter = Filters.and(
                                Filters.eq("symbol", row.getAs("symbol")),
                                Filters.eq("start_time", row.getAs("start_time")),
                                Filters.eq("end_time", row.getAs("end_time"))
                        );

                        // Thay vì gửi ngay, ta thêm vào hàng đợi bulkOperations
                        // Sử dụng ReplaceOneModel cho bulk write
                        bulkOperations.add(new ReplaceOneModel<>(filter, doc, upsertOptions));

                        // Khi đủ 1000 lệnh hoặc 2000 lệnh thì bắn 1 lần
                        if (bulkOperations.size() >= 1000) {
                                collection.bulkWrite(bulkOperations, bulkOptions);
                                bulkOperations.clear(); // Xóa list để gom mẻ mới
                        }
                        }

                        // Gửi nốt những lệnh còn sót lại (nếu < 1000)
                        if (!bulkOperations.isEmpty()) {
                        collection.bulkWrite(bulkOperations, bulkOptions);
                        }

                } catch (Exception e) {
                        System.err.println("Error in Bulk Write to Mongo: " + e.getMessage());
                        e.printStackTrace();
                }
                });
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