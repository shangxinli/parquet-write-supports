package org.apache.spark.sql.execution.datasources.parquet;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

public class CryptoParquetWriteSupportTest {

  @Test
  public void testCreateWriteSupport() {
    Configuration conf = new Configuration();
    String schemaString =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"price\",\"type\":\"long\","
            + "\"nullable\":true,\"metadata\":{\"encrypted\": true,"
            + "\"columnKeyMetaData\": \"AAA=\"}},{\"name\":\"product\","
            + "\"type\":\"string\",\"nullable\":true,\"metadata\":{\"encrypted\": true,"
            + "\"columnKeyMetaData\": \"AAA=\"}}]}";
    MessageType schema = convertAndGetSchema(conf, schemaString);
    List<Type> fileds = schema.getFields();
    assertNotNull(fileds);
    assertTrue(fileds.size() == 2);
    for (Type field : fileds) {
      assertTrue(field instanceof ExtType);
      ExtType extField = (ExtType) field;
      Map<String, Object> fieldMetadata = extField.getMetadata();
      assertTrue(fieldMetadata != null);
      assertTrue(fieldMetadata.size() == 2);
      assertTrue(fieldMetadata.get("encrypted").equals(true));
      assertTrue(fieldMetadata.get("columnKeyMetaData").equals("AAA="));
    }
  }

  @Test
  public void testCreateWriteSupportOneField() {
    Configuration conf = new Configuration();
    String schemaString =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"price\",\"type\":\"long\","
            + "\"nullable\":true,\"metadata\":{\"encrypted\": false,"
            + "\"columnKeyMetaData\": \"abcd\"}}]}";
    MessageType schema = convertAndGetSchema(conf, schemaString);
    List<Type> fileds = schema.getFields();
    assertNotNull(fileds);
    assertTrue(fileds.size() == 1);
    Type field = fileds.get(0);
    assertTrue(field instanceof ExtType);
    ExtType extField = (ExtType) field;
    Map<String, Object> fieldMetadata = extField.getMetadata();
    assertTrue(fieldMetadata != null);
    assertTrue(fieldMetadata.size() == 2);
    assertTrue(fieldMetadata.get("encrypted").equals(false));
    assertTrue(fieldMetadata.get("columnKeyMetaData").equals("abcd"));
  }

  private MessageType convertAndGetSchema(Configuration conf, String schemaString) {
    conf.set("org.apache.spark.sql.parquet.row.attributes", schemaString);
    conf.setBoolean("spark.sql.parquet.writeLegacyFormat", false);
    conf.set("spark.sql.parquet.outputTimestampType", "INT96");
    CryptoParquetWriteSupport writeSupport = new CryptoParquetWriteSupport();
    WriteContext writeContext = writeSupport.init(conf);
    return writeContext.getSchema();
  }
}
