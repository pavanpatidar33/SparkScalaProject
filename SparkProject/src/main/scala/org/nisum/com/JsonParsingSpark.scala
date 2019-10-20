package org.nisum.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.mutable.ListMap
import scala.util.parsing.json.JSON
import org.apache.spark.sql.functions.struct
object JsonPrsingSpark {
  def main(args: Array[String]) = {

    System.setProperty("hadoop.home.dir", "C:\\winutils\\");

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("JsonPrsingSpark")
      .getOrCreate()

    val record1Json: String = """{"visitorId":"v1","products":[{"id":"i1","interest":0.68},{"id":"i2","interest":0.42}]}"""
    val record2Json: String = """{"visitorId":"v2","products":[{"id":"i1","interest":0.78},{"id":"i3","interest":0.11}]}"""

    val visitorData :Seq[String] = Seq(record1Json,record2Json)
    var enrichedRec1=""
    var enrichedRec2=""
    
    visitorData.foreach(f=>{
      
        enrichedRec1= parseJsonSting(spark, record1Json)
        enrichedRec2= parseJsonSting(spark, record2Json)
    })
    
  
  
  val output : Seq[String] = Seq(enrichedRec1,enrichedRec2)
   
  println("Final output : "+output)
  }
  
  def parseJsonSting(spark: SparkSession,jsonString: String): String  = {

    val productIdMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella", "i3" -> "Jeans")
    import spark.implicits._

    val record1Df = spark.read.json(Seq(jsonString).toDS)

    val flattenedRecord = record1Df.select($"visitorId", explode($"products").as("products"))

    val productDetailsRecord = flattenedRecord.select("products.id", "products.interest")

    val df2 = productDetailsRecord.selectExpr("cast(interest as string) interest", "id")

    val productIdMapDF = productIdMap.toSeq.toDF("id", "name")

    val recordFinalDF = df2.join(productIdMapDF, Seq("id"))

    val recordWithProduct = recordFinalDF.withColumn("products", struct( recordFinalDF("id").as("id"), recordFinalDF("interest").as("interest"), recordFinalDF("name").as("name")))
      .drop("id").drop("name").drop("interest")

      println("*******************************************************************************")
    recordFinalDF.show()
    

    val visitorMap = flattenedRecord.drop("products").dropDuplicates()

    val dataframeWithoutAnyPrimaryKey1 = spark.sqlContext.createDataFrame(
      visitorMap.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(visitorMap.schema.fields :+ StructField("index", LongType, false)))

    val dataframeWithoutAnyPrimaryKey2 = spark.sqlContext.createDataFrame(
      recordWithProduct.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(recordWithProduct.schema.fields :+ StructField("index", LongType, false)))

    val enrichedRecDF = dataframeWithoutAnyPrimaryKey1.join(dataframeWithoutAnyPrimaryKey2, Seq("index")).drop("index")
   val enrichedRecArray = enrichedRecDF.collect.map(r => Map(enrichedRecDF.columns.zip(r.toSeq): _*))
   val enrichedRec1 = enrichedRecArray.toList
   enrichedRec1.toString()
  }

}