import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import com.datastax.oss.driver.api.core.uuid.Uuids

case class ExpenseRecord(user_id: Int, date_time: String, description: String, payment: Double)

object StreamHandler {
  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val spark = SparkSession
      .builder
      .appName("Expense Stream Handler")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    // Read from Kafka
    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribePattern", "user_.*_expenses")
      .load()

    // Parse the Kafka JSON values
    val schema = Encoders.product[ExpenseRecord].schema

    val expenseDF = inputDF
      .selectExpr("CAST(value AS STRING) as json_data")
      .select(from_json($"json_data", schema).as("data"))
      .select("data.*")

    // Add UUIDs for Cassandra
    val expenseWithID = expenseDF
      .withColumn("uuid", expr("uuid()"))

    // Write to Cassandra
    val query = expenseWithID
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"Writing batch $batchId to Cassandra")

        batchDF.write
          .cassandraFormat("expenses", "finance") // Table: expenses, Keyspace: finance
          .mode("append")
          .save()
      }
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
