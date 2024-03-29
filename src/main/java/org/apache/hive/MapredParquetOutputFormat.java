/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.parquet.write.CryptoDataWritableWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.ParquetOutputFormat;

/**
 *
 * A Parquet OutputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapredParquetOutputFormat extends FileOutputFormat<NullWritable, ParquetHiveRecord>
    implements HiveOutputFormat<NullWritable, ParquetHiveRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MapredParquetOutputFormat.class);
  public final static String PREFIX = "pdci_";
  public final static String HAS_ENCRYPTED_COLUMNS = "hasEncCols";

  protected ParquetOutputFormat<ParquetHiveRecord> realOutputFormat;

  public MapredParquetOutputFormat() {
    realOutputFormat = new ParquetOutputFormat<ParquetHiveRecord>(new DataWritableWriteSupport());
  }

  public MapredParquetOutputFormat(final OutputFormat<Void, ParquetHiveRecord> mapreduceOutputFormat) {
    realOutputFormat = (ParquetOutputFormat<ParquetHiveRecord>) mapreduceOutputFormat;
  }

  @Override
  public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {
    realOutputFormat.checkOutputSpecs(ShimLoader.getHadoopShims().getHCatShim().createJobContext(job, null));
  }

  @Override
  public RecordWriter<NullWritable, ParquetHiveRecord> getRecordWriter(
      final FileSystem ignored,
      final JobConf job,
      final String name,
      final Progressable progress
      ) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  /**
   *
   * Create the parquet schema from the hive schema, and return the RecordWriterWrapper which
   * contains the real output format
   */
  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      final JobConf jobConf,
      final Path finalOutPath,
      final Class<? extends Writable> valueClass,
      final boolean isCompressed,
      final Properties tableProperties,
      final Progressable progress) throws IOException {

    LOG.info("creating new record writer..." + this);
    List<String> columnNames = getColumnNames(tableProperties);
    List<TypeInfo> columnTypes = getColumnTypes(tableProperties);

    DataWritableWriteSupport.setSchema(HiveSchemaConverter.convert(columnNames, columnTypes), jobConf);
    setEncrMetaData(jobConf, tableProperties);

    if (jobConf.getBoolean(HAS_ENCRYPTED_COLUMNS, false)) {
      return getParquerRecordWriterWrapper(new ParquetOutputFormat<ParquetHiveRecord>(new CryptoDataWritableWriteSupport()),
              jobConf, finalOutPath.toString(), progress,tableProperties);
    } else {
      return getParquerRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(),
              progress,tableProperties);
    }
  }

  protected List<String> getColumnNames(final Properties tableProperties) {
    final String columnNameProperty = tableProperties.getProperty(IOConstants.COLUMNS);

    List<String> columnNames;

    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }

    return columnNames;
  }

  protected List<TypeInfo> getColumnTypes(final Properties tableProperties) {
    final String columnTypeProperty = tableProperties.getProperty(IOConstants.COLUMNS_TYPES);
    List<TypeInfo> columnTypes;

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    return columnTypes;

  }

  protected ParquetRecordWriterWrapper getParquerRecordWriterWrapper(
      ParquetOutputFormat<ParquetHiveRecord> realOutputFormat,
      JobConf jobConf,
      String finalOutPath,
      Progressable progress,
      Properties tableProperties
      ) throws IOException {
    return new ParquetRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(),
            progress,tableProperties);
  }

  /**
   * If a column is tagged for privacy/sensitivity, then there will be a corresponding table property in the schema
   * with the format "pdci_colname". For example, if the table property contains a map entry "pdci_column1" : "address",
   * it means the "column1" is tagged as "address" which is PII and need to be protected. When it is saved as Parquet
   * file, Parquet library with Parquet-1178/1396 will encrypt this column.
   */
  private void setEncrMetaData(final JobConf jobConf,
                               final Properties tableProperties) throws IOException {
    List<String> columnNames = getColumnNames(tableProperties);
    if (LOG.isDebugEnabled()) {
      LOG.debug("There are " + columnNames.size() + " columns found in tableProperties");
    }

    for (Object entry : tableProperties.keySet()) {
      if (entry instanceof String && ((String) entry).startsWith(PREFIX)) {
        String key = (String) entry;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found table property with pdci_ prefix: " + key + " with property: " +
                  tableProperties.getProperty(key));
        }
        String[] tokens = key.split(PREFIX);
        if (tokens.length < 2 || !columnNames.contains(key.substring(PREFIX.length()))) {
          /**
           * When a PDCI is added to table property, it is validated as correct format.
           * So if we reach here, it means it is corrupted.
           */
          throw new IOException("Found corrupted table property with key: " + key);
        }
        jobConf.set(key, tableProperties.getProperty(key));
        jobConf.setBoolean(HAS_ENCRYPTED_COLUMNS, true);
        if (LOG.isDebugEnabled()) {
          LOG.debug("jobConf is set with key: " + key + " value: " + tableProperties.getProperty(key));
        }
      }
    }
  }
}
