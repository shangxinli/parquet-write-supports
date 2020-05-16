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
package org.apache.hadoop.hive.ql.io.parquet.write;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 *
 * CryptoDataWritableWriteSupport is a WriteSupport for the DataWritableWriter.
 *
 * It extends DataWritableWriteSupport by adding the feature of carrying
 * over the column metadata to parquet schema if it presents.
 *
 */
public class CryptoDataWritableWriteSupport extends DataWritableWriteSupport {

    private final static Log LOG = LogFactory.getLog(CryptoDataWritableWriteSupport.class);

    @Override
    public WriteContext init(Configuration configuration) {
        WriteContext writeContext = super.init(configuration);
        MessageType schema = writeContext.getSchema();
        List<Type> fields = schema.getFields();
        List<Type> newFields = new ArrayList<>();

        Map<String, Map<String, String>> metadatas = getMetadatas(configuration, fields);

        if (LOG.isDebugEnabled()) {
            LOG.debug("There are " + fields.size() + " fields");
        }

        for (Type field : fields) {
            ExtType<String> cryptoField = convertToExtTypeField(metadatas, field);
            newFields.add(cryptoField);
        }

        MessageType newSchema = new MessageType(schema.getName(), newFields);
        Map<String, String> extraMetadata = new HashMap<>();

        return new WriteContext(newSchema, extraMetadata);
    }

    private ExtType<String> convertToExtTypeField(Map<String, Map<String, String>> metadatas, Type field) {
        if (field.isPrimitive()) {
            ExtType<String> result = new ExtType<>(field);
            if (metadatas.containsKey(field.getName())) {
                result.setMetadata(metadatas.get(field.getName()));
            }
            return result;
        } else {
            List<Type> newFields = new ArrayList<>();
            for (Type childField : field.asGroupType().getFields()) {
                ExtType<String> newField = convertToExtTypeField(metadatas, childField);
                newFields.add(newField);
            }
            ExtType<String> result = new ExtType<>(field.asGroupType().withNewFields(newFields));
            result.setMetadata(metadatas.get(field.getName()));
            return result;
        }
    }

    private Map<String, Map<String, String>> getMetadatas(Configuration configuration, List<Type> fields) {
        // The key of the map is the column name, and the value is with type of Map which has key 'pdci_'
        Map<String, Map<String, String>> metadatas = new HashMap<>();
        for (Type field : fields) {
            String colMetadata = MapredParquetOutputFormat.PREFIX + field.getName();
            if (configuration.get(colMetadata) != null) {
                Map<String, String> metadata = new HashMap<>();
                metadata.put(MapredParquetOutputFormat.PREFIX, configuration.get(colMetadata));
                metadata.put("columnKeyMetaData", configuration.get(colMetadata));
                metadata.put("encrypted", "true");
                metadatas.put(field.getName(), metadata);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Field: " + field.getName() + " is set metadata");
                }
            }
        }
        return metadatas;
    }
}
