package spark;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.parquet.format.DateType;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.apache.spark.sql.functions.*;

public class Spark_Consumer_Data {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException, java.util.concurrent.TimeoutException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Streaming with Kafka")
                .master("local")
//                .config("spark.dynamicAllocation.enabled","false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ReadStream(spark);
    }

    private static void ReadStream(SparkSession spark) throws TimeoutException, StreamingQueryException, java.util.concurrent.TimeoutException {
        spark.udf().register("deserialize", (byte[] value) -> {
            String strValue = new String(value, StandardCharsets.UTF_8);
            return strValue;
        }, DataTypes.StringType);

        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.193.118:9092")
                .option("subscribe", "tiencm8")
                .option("group.id","group2")
                .option("StartingOffsets", "earliest")
                .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .load();

        Dataset<Row> valueDf = df.selectExpr("deserialize(value) as value");


        Dataset<Row> splitDf = valueDf.select(
                split(valueDf.col("value"),",").getItem(0).cast(DataTypes.StringType).as("id"),
                split(valueDf.col("value"),",").getItem(1).cast(DataTypes.StringType).as("name"),
                split(valueDf.col("value"),",").getItem(2).cast(DataTypes.StringType).as("author_name"),
                split(valueDf.col("value"),",").getItem(3).cast(DataTypes.StringType).as("inventory_status"),
                split(valueDf.col("value"),",").getItem(4).cast(DataTypes.IntegerType).as("original_price"),
                split(valueDf.col("value"),",").getItem(5).cast(DataTypes.IntegerType).as("discount"),
                split(valueDf.col("value"),",").getItem(6).cast(DataTypes.IntegerType).as("price"),
                split(valueDf.col("value"),",").getItem(7).cast(DataTypes.IntegerType).as("discount_rate"),
                split(valueDf.col("value"),",").getItem(8).cast(DataTypes.FloatType).as("rating_average"),
                split(valueDf.col("value"),",").getItem(9).cast(DataTypes.IntegerType).as("review_count"),
                split(valueDf.col("value"),",").getItem(10).cast(DataTypes.IntegerType).as("quantity_sold"),
                split(valueDf.col("value"),",").getItem(11).cast(DataTypes.StringType).as("type_book"),
                split(valueDf.col("value"),",").getItem(12).cast(DataTypes.StringType).as("book_cover")
        );


        VoidFunction2<Dataset<Row>, Long> saveFunction = new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                rowDataset.write()
                        .mode("append")
                        .format("jdbc")
                        .option("url", "jdbc:mysql://localhost:8000/tiki_data")
                        .option("dbtable","product")
                        .option("user","root")
                        .option("password","Tiencm")
                        .option("driver","com.mysql.cj.jdbc.Driver")
                        .save();
            }
        };

        StreamingQuery query = splitDf.writeStream()
                .outputMode("append")
                //.format("")
                //.option("path", "E:/FinalProject-Masterdev-Domain_DATA/Final_Project/data")
                .foreachBatch(saveFunction)
                .option("checkpointLocation","checkpoint/")
                .trigger(Trigger.ProcessingTime(1000))
                .start();

        query.awaitTermination();

    }
}
