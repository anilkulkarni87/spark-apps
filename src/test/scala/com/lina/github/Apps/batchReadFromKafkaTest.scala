package com.lina.github.Apps

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class batchReadFromKafkaTest extends FunSpec with BeforeAndAfterEach with SharedSparkContext {
  var sparkSession: SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession
      .builder()
      .appName("LocalTest")
      .master("local")
      .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .config("parquet.summary.metadata.level", "NONE")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

  }

  override def afterEach() {
    sparkSession.stop()
    super.afterAll()
  }

  describe("Collection of Tests") {
    it("Test 1") {}
  }

}
