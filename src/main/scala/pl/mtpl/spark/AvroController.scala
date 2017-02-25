package pl.mtpl.spark

import org.apache.spark.rdd.RDD
import com.databricks.spark.avro._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by MarcinT.P on 2017-02-25.
  */
class AvroController() {
  private def loadRDD(session: SparkSession, csvFileName: String) : RDD[Row] = {
    val peopleRDD: RDD[String] = session.sparkContext.textFile(csvFileName)

    // Convert records of the RDD (people) to Rows
    peopleRDD
      .map(_.split(";"))
      .map(cols => Row(cols(0),
          cols(1),
          cols(2),
          cols(3),
          cols(4))
      )
  }

  def save(csvFileName: String) : Unit = {

    val session: SparkSession = SparkSession.builder()
      .appName("Avro Persister")
      .getOrCreate()

    val rowRDD: RDD[Row] = loadRDD(session, csvFileName)

    // Generate the schema based on the string of schema
    val schema = StructType("no name surname is_male age"
      .split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = false))
    )

    // Apply the schema to the RDD
    val peopleDF = session.sqlContext.createDataFrame(rowRDD, schema)
    peopleDF.show(10)
    //val dst: String = s"${sys.env("TEMP")}/${csvFileName}avro"
    val dst: String = s"${csvFileName}avro"
    println(s"Writing via avro to $dst")
    peopleDF.write.mode(SaveMode.Overwrite).avro(dst)
    session.close()
  }
}
