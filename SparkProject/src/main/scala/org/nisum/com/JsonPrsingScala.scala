package org.nisum.com
import scala.collection.immutable.Map
import scala.util.parsing.json.JSON

import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
object JsonPrsingScala {
  def main(args: Array[String]): Unit = {
    val record1Json: String = """{"visitorId":"v1","products":[{"id":"i1","interest":0.68},{"id":"i2","interest":0.42}]}"""
    val record2Json: String = """{"visitorId":"v2","products":[{"id":"i1","interest":0.78},{"id":"i3","interest":0.11}]}"""

    val visitorData: Seq[String] = Seq(record1Json, record2Json)
    var enrichedRec1 = ""
    var enrichedRec2 = ""
    implicit val formats = Serialization.formats(NoTypeHints)
    visitorData.foreach(f => {

      val productmapwithVisitorRecord1 = parseJsonSting(record1Json)
      val productmapwithVisitorRecord2 = parseJsonSting(record2Json)
      enrichedRec1 = write(productmapwithVisitorRecord1)
      enrichedRec2 = write(productmapwithVisitorRecord2)
    })

    println("enrichedRec1 =====>  " + enrichedRec1)
    println("---------------------------------------------------")
    println("enrichedRec2 =====>  " + enrichedRec2)
    println("*************************************************************************************")
    val output: Seq[String] = Seq(enrichedRec1, enrichedRec2)
    println("Final Output  :-  " + output)
  }

  def parseJsonSting(jsonString: String): Map[String, Object] = {
    val record = JSON.parseFull(jsonString)
    val recordMap = record.get.asInstanceOf[Map[String, String]]

    val productIdMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella", "i3" -> "Jeans")

    val nestedJsonProductRecord = record.get.asInstanceOf[Map[String, Any]]("products")
    val productsMapRecord = nestedJsonProductRecord.asInstanceOf[List[Map[String, Any]]]

    val visitorMap = recordMap.filterKeys(_ != "products")

    var recordList = List[Map[String, String]]()

    productsMapRecord.foreach { product =>
      {
        val productMap = product.asInstanceOf[Map[String, String]]
        val keyOfProduct = productMap.get("id").get
        val productName = productIdMap.get(keyOfProduct).get
        val mapwithProduct = productMap + ("Name" -> productName)

        recordList = mapwithProduct :: recordList

      }
    }

    val productmapwithVisitorRecord = visitorMap + ("products" -> recordList)
    productmapwithVisitorRecord
  }
}