package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.spark.sql.types.StructType

class CryptoParquetWriteSupport extends ParquetWriteSupport {

  override def init(configuration: Configuration): WriteContext = {
    val context = super.init(configuration)

    // We need to re-create the messageType because init does not use our custom converter,
    // ParquetMetadataSchemaConverter. This is a work-around that lets us use a custom converter
    // instead of being forced to use the default converter in super.init().
    val schemaString = configuration.get(ParquetWriteSupport.SPARK_ROW_SCHEMA)
    val schema = StructType.fromString(schemaString)
    val messageType = new ParquetMetadataSchemaConverter(configuration).convert(schema)

    new WriteContext(messageType, context.getExtraMetaData)
  }
}
