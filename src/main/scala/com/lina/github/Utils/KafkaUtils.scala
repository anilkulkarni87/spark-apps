package com.lina.github.Utils

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.security.basicauth.{
  BasicAuthCredentialProvider,
  UserInfoCredentialProvider
}
import scala.collection.JavaConversions._
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

object KafkaUtils {

  def getSchemaFromSchemaRegistry(
      schemaRegistryURL: String,
      accessKey: String,
      secret: String,
      topicName: String
  ): Schema = {
    val subjectValueName = topicName + "-value"
    //create RestService object
    val restService                           = new RestService(schemaRegistryURL)
    val provider: BasicAuthCredentialProvider = new UserInfoCredentialProvider()

    provider.configure(
      Map(
        "schema.registry.basic.auth.credentials.source" -> "USER_INFO",
        "schema.registry.basic.auth.user.info"          -> (accessKey + ":" + secret)
      )
    )
    restService.setBasicAuthCredentialProvider(provider)

    val valueRestResponseSchema = restService.getLatestVersion(subjectValueName)
    val parser                  = new Schema.Parser
    val schemaObj: Schema       = parser.parse(valueRestResponseSchema.getSchema)
    schemaObj
  }

  def fromAvroToRow(
      dataframe: Dataset[Row],
      schemaRegistryUrl: String,
      schemaString: String,
      accessKey: String,
      secret: String,
      columnName: String
  ): Dataset[Row] = {

    // setting properties outside mapPartitions gives Serialization Exception
    dataframe.mapPartitions(partitions => {
      val parser        = new Schema.Parser()
      val avroSqlSchema = parser.parse(schemaString)
      val avroRecordConverter = AvroSchemaConverter
        .createConverterToSQL(avroSqlSchema, SchemaConverters.toSqlType(avroSqlSchema).dataType)

      val restService                           = new RestService(schemaRegistryUrl)
      val provider: BasicAuthCredentialProvider = new UserInfoCredentialProvider()
      provider.configure(
        Map(
          "schema.registry.basic.auth.credentials.source" -> "USER_INFO",
          "schema.registry.basic.auth.user.info"          -> (accessKey + ":" + secret)
        )
      )
      restService.setBasicAuthCredentialProvider(provider)
      val schemaRegistry: CachedSchemaRegistryClient =
        new CachedSchemaRegistryClient(restService, 1000)

      val decoder: KafkaAvroDecoder = new KafkaAvroDecoder(schemaRegistry)

      partitions.map(input => {
        val record =
          decoder.fromBytes(input.getAs(columnName), avroSqlSchema).asInstanceOf[GenericData.Record]
        // Convert record into a DataFrame with columns according to the schema
        avroRecordConverter(record).asInstanceOf[Row]
      })
    })(
      RowEncoder(
        SchemaConverters
          .toSqlType(new Schema.Parser().parse(schemaString))
          .dataType
          .asInstanceOf[StructType]
      )
    )

  }

}
