package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema._
import org.apache.spark.sql.types._
import org.junit.Assert.{assertNotNull, _}
import org.junit.Test

import scala.collection.JavaConverters._

class ParquetMetadataSchemaConverterTest {

  @Test
  def convertNonNestedSchemaWithoutMetadata(): Unit = {
    val converter = createConverter(false)
    assertNotNull(converter)

    val flatFields = List(
      StructField("number", IntegerType, true, Metadata.empty),
      StructField("word", StringType, true, Metadata.empty))
    val parquetSchema = converter.convert(StructType(flatFields))
    assertNotNull(parquetSchema)
    val parquetChildren = parquetSchema.asGroupType().getFields()
    assertEquals(parquetChildren.size(), 2)

    for (i <- 0 to parquetChildren.size - 1) {
      val column = parquetChildren.get(i)
      val columnName = column.getName
      val originalName = flatFields(i).name
      assertEquals(columnName, originalName)
      assertFalse(column.isInstanceOf[ExtType[Any]])
    }
  }

  @Test
  def convertNestedSchemaWithoutMetadata(): Unit = {
    val converter = createConverter(true)
    assertNotNull(converter)

    val childFields = List(
      StructField("number", IntegerType, true, Metadata.empty),
      StructField("word", StringType, true, Metadata.empty))

    val sparkSchema = List(
      StructField("parent", StructType(childFields), true, Metadata.empty),
      StructField("uncle", StringType, true, Metadata.empty),
      StructField("aunt", ByteType, true, Metadata.empty))

    val parquetSchema = converter.convert(StructType(sparkSchema))
    assertNotNull(parquetSchema)
    val parquetFields = parquetSchema.asGroupType().getFields()
    assertEquals(parquetFields.size(), 3)

    for (i <- 0 to parquetFields.size - 1) {
      val column = parquetFields.get(i)
      assertFalse(column.isInstanceOf[ExtType[Any]])
      if (column.getName.equals("parent")) {
        assertEquals(column.asGroupType().getFieldCount, 2)
        val parquetChildren = column.asGroupType().getFields
        for (j <- 0 to parquetChildren.size - 1) {
          assertFalse(parquetChildren.get(j).isInstanceOf[ExtType[Any]])
        }
      }
    }
  }

  @Test
  def convertNonNestedSchemaWithMetadata(): Unit = {
    val converter = createConverter(true)
    assertNotNull(converter)

    val flatFields = List(
      StructField(
        "number",
        IntegerType,
        true,
        Metadata.fromJson(
          "{\"metadata\":{\"encrypted\": true,\"columnKeyMetaData\": \"db0.tbl1.col2\"}}")),
      StructField(
        "word",
        StringType,
        true,
        Metadata.fromJson("{\"metadata\":{\"encrypted\": false}}")))
    val parquetSchema = converter.convert(StructType(flatFields))
    assertNotNull(parquetSchema)
    val parquetChildren = parquetSchema.asGroupType().getFields()
    assertEquals(parquetChildren.size(), 2)

    validateLeafChildren(flatFields, parquetChildren)
  }

  @Test
  def convertNestedSchemaWithMetadata(): Unit = {
    val converter = createConverter(false)
    assertNotNull(converter)

    val childFields = List(
      StructField(
        "number",
        IntegerType,
        true,
        Metadata.fromJson(
          "{\"metadata\":{\"encrypted\": true,\"columnKeyMetaData\": \"db0.tbl1.col2\"}}")),
      StructField(
        "word",
        StringType,
        true,
        Metadata.fromJson("{\"metadata\":{\"encrypted\": false}}")))

    val sparkSchema = List(
      StructField("parent", StructType(childFields), true, Metadata.empty),
      StructField(
        "uncle",
        StringType,
        true,
        Metadata.fromJson("{\"metadata\":{\"encrypted\": false}}")),
      StructField("aunt", ByteType, true, Metadata.empty))

    val parquetSchema = converter.convert(StructType(sparkSchema))
    assertNotNull(parquetSchema)
    val parquetFields = parquetSchema.asGroupType().getFields()
    assertEquals(parquetFields.size(), 3)

    for (i <- 0 to parquetFields.size - 1) {
      val column = parquetFields.get(i)
      if (column.getName.equals("parent")) {
        assertEquals(column.asGroupType().getFieldCount, 2)
        val parquetChildren = column.asGroupType().getFields
        validateLeafChildren(childFields, parquetChildren)
      } else if (column.getName.equals("uncle")) {
        val parquetMetadata = column.asInstanceOf[ExtType[Any]].getMetadata.asScala
        val sparkMetadata = new ExtMetadataBuilder().withMetadata(sparkSchema(i).metadata).getMap
        assertEquals(parquetMetadata, sparkMetadata)
      } else if (column.getName.equals("aunt")) {
        assertFalse(column.isInstanceOf[ExtType[Any]])
      }
    }
  }

  @Test
  def convertArraySchemaWithoutMetadata(): Unit = {
    convertArraySchemaWithoutMetadataHelper(createConverter(true))
    convertArraySchemaWithoutMetadataHelper(createConverter(false))
  }

  def convertArraySchemaWithoutMetadataHelper(converter: SparkToParquetSchemaConverter): Unit = {
    assertNotNull(converter)

    val sparkSchema = Array(
      StructField("id", StringType, false),
      StructField("emailBody", StringType, false),
      StructField("sentTo", ArrayType(StringType, true)),
      StructField("ccTo", ArrayType(StringType, false)))

    val parquetSchema = converter.convert(StructType(sparkSchema))
    assertNotNull(parquetSchema)
    val parquetFields = parquetSchema.asGroupType().getFields()
    assertEquals(parquetFields.size, 4)

    for (i <- 0 to parquetFields.size - 1) {
      val column = parquetFields.get(i)
      assertFalse(column.isInstanceOf[ExtType[Any]])
      if (column.getName.equals("sentTo") || column.getName.equals("ccTo")) {
        assertEquals(column.asGroupType().getFieldCount, 1)
        val parquetChildren = column.asGroupType().getFields
        for (j <- 0 to parquetChildren.size - 1) {
          assertFalse(parquetChildren.get(j).isInstanceOf[ExtType[Any]])
          if (!parquetChildren.get(j).isPrimitive) {
            val parquetGrantChildren = parquetChildren.get(j).asGroupType().getFields
            for (k <- 0 to parquetGrantChildren.size() - 1) {
              assertFalse(parquetGrantChildren.get(k).isInstanceOf[ExtType[Any]])
            }
          }
        }
      }
    }
  }

  @Test
  def convertArraychemaWithMetadata(): Unit = {
    convertArraychemaWithMetadataHelper(createConverter(true))
    convertArraychemaWithMetadataHelper(createConverter(false))
  }

  def convertArraychemaWithMetadataHelper(converter: SparkToParquetSchemaConverter): Unit = {
    assertNotNull(converter)

    val sparkSchema = Array(
      StructField("id", StringType, false),
      StructField(
        "emailBody",
        StringType,
        false,
        Metadata.fromJson("{\"metadata\":{\"encrypted\": false}}")),
      StructField(
        "sentTo",
        ArrayType(StringType, true),
        false,
        Metadata.fromJson(
          "{\"metadata\":{\"encrypted\": true,\"columnKeyMetaData\": \"db0.tbl1.col2\"}}")),
      StructField(
        "ccTo",
        ArrayType(StringType, false),
        false,
        Metadata.fromJson(
          "{\"metadata\":{\"encrypted\": true,\"columnKeyMetaData\": \"db0.tbl1.col2\"}}")))

    val parquetSchema = converter.convert(StructType(sparkSchema))
    assertNotNull(parquetSchema)
    val parquetFields = parquetSchema.asGroupType().getFields()
    assertEquals(parquetFields.size, 4)

    for (i <- 0 to parquetFields.size - 1) {
      val column = parquetFields.get(i)
      if (column.getName.equals("sentTo") || column.getName.equals("ccTo")) {
        assertEquals(column.asGroupType().getFieldCount, 1)
        val parquetChildren = column.asGroupType().getFields
        for (j <- 0 to parquetChildren.size - 1) {
          if (parquetChildren.get(j).isPrimitive) {
            assertTrue(parquetChildren.get(j).isInstanceOf[ExtType[Any]])
          } else {
            assertFalse(parquetChildren.get(j).isInstanceOf[ExtType[Any]])
            val parquetGrantChildren = parquetChildren.get(j).asGroupType().getFields
            for (k <- 0 to parquetGrantChildren.size() - 1) {
              assertTrue(parquetGrantChildren.get(k).isInstanceOf[ExtType[Any]])
            }
          }
        }
      } else if (column.getName.equals("emailBody")) {} else { // id

      }
    }
  }

  @Test
  def convertMapSchemaWithoutMetadata(): Unit = {
    convertMapSchemaWithoutMetadataHelper(createConverter(true))
    convertMapSchemaWithoutMetadataHelper(createConverter(false))
  }

  def convertMapSchemaWithoutMetadataHelper(converter: SparkToParquetSchemaConverter): Unit = {
    assertNotNull(converter)

    val sparkSchema = Array(
      StructField("xyz", StringType, false),
      StructField("mtz", MapType(StringType, StringType, valueContainsNull = false)))

    val parquetSchema = converter.convert(StructType(sparkSchema))
    assertNotNull(parquetSchema)
    val parquetFields = parquetSchema.asGroupType().getFields()
    assertEquals(parquetFields.size, 2)

    for (i <- 0 to parquetFields.size - 1) {
      val column = parquetFields.get(i)
      assertFalse(column.isInstanceOf[ExtType[Any]])
      if (column.getName.equals("mtz")) {
        assertEquals(column.asGroupType().getFieldCount, 1)
        val parquetChildren = column.asGroupType().getFields
        for (j <- 0 to parquetChildren.size - 1) {
          assertFalse(parquetChildren.get(j).isInstanceOf[ExtType[Any]])
          assertFalse(parquetChildren.get(j).isInstanceOf[ExtType[Any]])
          val parquetGrantChildren = parquetChildren.get(j).asGroupType().getFields
          for (k <- 0 to parquetGrantChildren.size() - 1) {
            assertFalse(parquetGrantChildren.get(k).isInstanceOf[ExtType[Any]])
          }
        }
      }
    }
  }

  private def createConverter(writeLegacyFormat: Boolean): ParquetMetadataSchemaConverter = {
    val conf = new Configuration
    conf.setBoolean("spark.sql.parquet.writeLegacyFormat", writeLegacyFormat)
    conf.set("spark.sql.parquet.outputTimestampType", "INT96")
    return new ParquetMetadataSchemaConverter(conf)
  }

  // All leaf nodes are with ExtType
  private def validateLeafChildren(
      sparkFields: List[StructField],
      parquetFields: java.util.List[Type]): Unit = {
    for (i <- 0 to parquetFields.size - 1) {
      val column = parquetFields.get(i)
      val columnName = column.getName
      val originalName = sparkFields(i).name
      assertEquals(columnName, originalName)

      val parquetMetadata = column.asInstanceOf[ExtType[Any]].getMetadata.asScala
      val sparkMetadata = new ExtMetadataBuilder().withMetadata(sparkFields(i).metadata).getMap
      assertEquals(parquetMetadata, sparkMetadata)
    }
  }
}
