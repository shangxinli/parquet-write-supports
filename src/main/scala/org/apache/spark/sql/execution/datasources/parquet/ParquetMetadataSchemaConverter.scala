package org.apache.spark.sql.execution.datasources.parquet

import java.util
import java.util.Map

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.{ExtType, MessageType, Type, Types}
import org.apache.spark.sql.types.{ArrayType, MapType, MetadataBuilder, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * This class pass field's metadata in StructType to the field of MessageType in addition to
  * existing functions of ParquetSchemaConverter.
  *
  * It has dependency on the class ExtType defined in the link below. Parquet-1396 is opened to merge
  * ExtType to Parquet-mr repo. https://github.com/shangxinli/parquet-mr/blob/encryption/parquet-column/
  * src/main/java/org/apache/parquet/schema/ExtType.java
  *
  */
class ParquetMetadataSchemaConverter(conf: Configuration)
    extends SparkToParquetSchemaConverter(conf) {

  private val log = LoggerFactory.getLogger(classOf[ParquetMetadataSchemaConverter])

  /**
    * Converts a Spark SQL [[StructField]] to a Parquet [[Type]].
    */
  override def convert(catalystSchema: StructType): MessageType = {
    Types
      .buildMessage()
      .addFields(catalystSchema.map(convertFieldWithMetadata): _*)
      .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  }

  private def convertFieldWithMetadata(sparkField: StructField): Type = {
    /*
     * It is somewhat weird that we take different approach to convert from StructField to ExtType; This is because
     * we want to utilize existing spark converter (convertField) which is not trivial to implement.
     */
    val parquetField = convertField(sparkField)
    if (hasMetadata(sparkField)) {
      log.debug("sparkField {} has metadata included", sparkField.name)
      val (fieldWithMeta, withMetadata) = convertToExtTypeField(parquetField, sparkField)
      if (!withMetadata) throw new RuntimeException("No metadata was found")
      return fieldWithMeta
    } else {
      log.debug("sparkField {} doesn't have metadata included", sparkField.name)
      return parquetField
    }
  }

  /**
    * Only leaf node(field) with metadata can have ExtType[Any].
    */
  private def convertToExtTypeField(
      parquetField: Type,
      sparkField: StructField,
      columnName: Option[String] = None): (Type, Boolean) = {
    if (parquetField.isPrimitive) {
      val metadata = getMetadata(sparkField, columnName.getOrElse(parquetField.getName))
      if (metadata == null || metadata.size() == 0) return (parquetField, false)
      log.info("Column {} is converted to ExtType", parquetField.getName)
      val extField = new ExtType[Any](parquetField)
      extField.setMetadata(metadata)
      return (extField, true)
    } else {
      val childFields = new util.ArrayList[Type]
      var withMetadata: Boolean = false
      parquetField.asGroupType().getFields.foreach { childField =>
        val (converted, hasMetadata) = convertToExtTypeField(
          childField,
          sparkField,
          geColumnSearchName(parquetField, sparkField, childField.getName, columnName))
        withMetadata = withMetadata || hasMetadata
        childFields.add(converted)
      }
      if (withMetadata) {
        val newField = parquetField.asGroupType().withNewFields(childFields)
        return (newField, true)
      } else {
        return (parquetField, false)
      }
    }
  }

  /**
    * This function find the right column name to search in sparkField for whether or not having metadata in leaf node.
    *
    * 1) If the passed in columnName is not None, then use it.
    * 2) Else if sparkField is complex datatype(Map, Array), parquetField is used.
    * 3) Else use childName.
    *
    * For the reason why we have the above logic, please refer to SparkToParquetSchemaConverter#convertField in Spark code base.
    */
  private def geColumnSearchName(
      parquetField: Type,
      sparkField: StructField,
      childName: String,
      columnName: Option[String]): Option[String] = {
    val searchedName = columnName.getOrElse(getFieldName(parquetField, sparkField, childName))
    log.debug("Column Search name is for sparkField")
    Option(searchedName)
  }

  private def getFieldName(
      parquetField: Type,
      sparkField: StructField,
      childName: String): String = {
    if (isComplexDataType(sparkField)) parquetField.getName
    else childName
  }

  private def isComplexDataType(sparkField: StructField): Boolean = {
    return sparkField.dataType.isInstanceOf[ArrayType] || sparkField.dataType.isInstanceOf[MapType]
  }

  /**
    * Recursively check if there is metadata in any leaf node of the tree with root sparkField
    */
  private def hasMetadata(sparkField: StructField): Boolean = {
    sparkField.dataType match {
      case StructType(children) =>
        for (child <- children) {
          if (hasMetadata(child)) return true
        }
        return false;

      case _ =>
        val metadata = new ExtMetadataBuilder().withMetadata(sparkField.metadata).getMap
        return metadata != null && metadata.size != 0
    }
  }

  /**
    * Get the metadata inside the tree with root sparkField under the name 'name'.
    *
    * TODO: we only compare name for now instead of (name, type, repetition); To compare all 3,
    * we need clone the logic of convertField() inside ParquetSchemaConverter.scala in Spark code.
    */
  private def getMetadata(sparkField: StructField, name: String): Map[String, Any] = {
    sparkField.dataType match {
      case StructType(children) =>
        for (child <- children) {
          val metadata = getMetadata(child, name)
          if (metadata != null) {
            return metadata
          }
        }
        return null;

      case _ =>
        if (sparkField.name.equals(name)) {
          val metaBuilder = new ExtMetadataBuilder().withMetadata(sparkField.metadata)
          return metaBuilder.getMap
        } else {
          return null
        }
    }
  }
}

/**
  * Due to the access modifier of getMap() in Spark, ExtMetadataBuilder is created to let getMap can be
  * accessed in above class.
  */
class ExtMetadataBuilder extends MetadataBuilder {

  override def getMap = {
    super.getMap
  }
}
