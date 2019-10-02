package com.lina.github.Apps

import com.lina.github.Utils.KafkaUtils
import org.apache.spark.sql.SparkSession

object batchReadFromKafka {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("Batch Read from Kafka")
      .master("local")
      .getOrCreate()

    val schemaRegistryURL =""
    val accessKey=""
    val secret=""
    val topic =""

    val kafkaBatchDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "")
      .option("kafka.security.protocol","sasl_ssl")
      .option("sasl.username", accessKey)
      .option("sasl.password", secret)
      .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"<username>>\" password=\"<password>>\";")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("failOnDataLoss","false")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
      .option("sasl.kerberos.service.name","kafka")
      .option("subscribe", topic)
      .load()

    val avroSchema = KafkaUtils.getSchemaFromSchemaRegistry(schemaRegistryURL,accessKey,secret, topic)
    print(avroSchema)
    val next = KafkaUtils.fromAvroToRow(kafkaBatchDF,schemaRegistryURL,avroSchema.toString(), accessKey, secret, "value")
    next.show()
  }



}
