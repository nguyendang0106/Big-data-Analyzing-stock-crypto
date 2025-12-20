package tn.insat.tp3;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Test script để verify GCS connection và write
 */
public class TestGCSConnection {

    public static void main(String[] args) {
        
        System.out.println(" Testing GCS Connection...\n");

        // Load environment variables
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        String gcsKeyPath = dotenv.get("GCS_KEY_PATH");
        String gcsBucket = dotenv.get("GCS_BUCKET");
        String gcsRawPath = dotenv.get("GCS_RAW_PATH", "test-data");

        if (gcsKeyPath == null || gcsBucket == null) {
            System.err.println(" GCS environment variables not set!");
            System.err.println("   Please set: GCS_KEY_PATH, GCS_BUCKET in .env file");
            System.exit(1);
        }

        System.out.println(" Configuration:");
        System.out.println("   GCS Key Path: " + gcsKeyPath);
        System.out.println("   GCS Bucket: " + gcsBucket);
        System.out.println("   GCS Path: " + gcsRawPath);
        System.out.println();

        // Create Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("GCS-Connection-Test")
                .master("local[*]")
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcsKeyPath)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        try {
            System.out.println(" Creating test data...");
            
            // Create sample data
            StructType schema = new StructType()
                    .add("id", DataTypes.IntegerType)
                    .add("symbol", DataTypes.StringType)
                    .add("price", DataTypes.DoubleType)
                    .add("timestamp", DataTypes.TimestampType);

            // List<Row> data = Arrays.asList(
            //     spark.createDataFrame(Arrays.asList(
            //         org.apache.spark.sql.RowFactory.create(1, "BTCUSDT", 45000.5, 
            //             java.sql.Timestamp.valueOf("2024-12-20 10:00:00")),
            //         org.apache.spark.sql.RowFactory.create(2, "ETHUSDT", 2300.75, 
            //             java.sql.Timestamp.valueOf("2024-12-20 10:00:01")),
            //         org.apache.spark.sql.RowFactory.create(3, "BTCUSDT", 45001.2, 
            //             java.sql.Timestamp.valueOf("2024-12-20 10:00:02"))
            //     ), schema).collectAsList()
            // );

            Dataset<Row> testDF = spark.createDataFrame(
                Arrays.asList(
                    org.apache.spark.sql.RowFactory.create(1, "BTCUSDT", 45000.5, 
                        java.sql.Timestamp.valueOf("2024-12-20 10:00:00")),
                    org.apache.spark.sql.RowFactory.create(2, "ETHUSDT", 2300.75, 
                        java.sql.Timestamp.valueOf("2024-12-20 10:00:01")),
                    org.apache.spark.sql.RowFactory.create(3, "BTCUSDT", 45001.2, 
                        java.sql.Timestamp.valueOf("2024-12-20 10:00:02"))
                ), 
                schema
            );

            System.out.println(" Test data created:");
            testDF.show();

            // Write to GCS
            String gcsPath = String.format("gs://%s/%s/test-run", gcsBucket, gcsRawPath);
            System.out.println("\n Writing to GCS: " + gcsPath);

            testDF.write()
                    .mode("overwrite")
                    .partitionBy("symbol")
                    .parquet(gcsPath);

            System.out.println(" Data written successfully!");

            // Read back from GCS
            System.out.println("\n Reading back from GCS...");
            Dataset<Row> readDF = spark.read().parquet(gcsPath);
            
            System.out.println(" Data read successfully:");
            readDF.show();

            long count = readDF.count();
            System.out.println(" Total records: " + count);

            if (count == 3) {
                System.out.println("\n GCS CONNECTION TEST PASSED! ");
                System.out.println("Your GCS setup is working correctly!");
            } else {
                System.out.println("\n  Warning: Expected 3 records but got " + count);
            }

            // Cleanup test data (optional)
            System.out.println("\n Cleaning up test data...");
            try {
                spark.read().parquet(gcsPath).write().mode("overwrite").parquet(gcsPath + "_deleted");
                System.out.println(" Test data cleaned");
            } catch (Exception e) {
                System.out.println("  Test data will remain in GCS (can be deleted manually)");
            }

        } catch (Exception e) {
            System.err.println("\n TEST FAILED!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }

        System.out.println("\n Test completed successfully!");
    }
}