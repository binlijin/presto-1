/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.druid.ingestion;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.prestosql.druid.DruidConfig;
import io.prestosql.druid.metadata.DruidColumnInfo;
import io.prestosql.druid.metadata.DruidColumnType;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static io.prestosql.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DruidPageWriter
{
    public static final JsonFactory JSON_FACTORY = new JsonFactory();
    public static final String DATA_FILE_EXTENSION = ".json.gz";

    private final Configuration hadoopConfiguration;
    private final DruidConfig druidConfig;

    @Inject
    public DruidPageWriter(DruidConfig druidConfig)
    {
        this.druidConfig = requireNonNull(druidConfig, "druidConfig is null");
        this.hadoopConfiguration = druidConfig.readHadoopConfiguration();
    }

    public Path append(Page page, DruidIngestionTableHandle tableHandle, Path dataPath)
    {
        Path dataFile = new Path(dataPath, UUID.randomUUID() + DATA_FILE_EXTENSION);
        try {
            FileSystem fileSystem = dataFile.getFileSystem(hadoopConfiguration);
            try (FSDataOutputStream outputStream = fileSystem.create(dataFile);
                    GZIPOutputStream zipOutputStream = new GZIPOutputStream(outputStream);
                    JsonGenerator jsonGen = JSON_FACTORY.createGenerator(zipOutputStream)) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    jsonGen.writeStartObject();
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        DruidColumnInfo column = tableHandle.getColumns().get(channel);
                        Block block = page.getBlock(channel);
                        jsonGen.writeFieldName(column.getColumnName());
                        writeFieldValue(jsonGen, column.getDataType(), block, position);
                    }
                    jsonGen.writeEndObject();
                }
            }
            return dataFile;
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, "Ingestion failed on " + tableHandle.getTableName(), e);
        }
    }

    private void writeFieldValue(JsonGenerator jsonGen, DruidColumnType dataType, Block block, int position)
            throws IOException
    {
        switch (dataType) {
            case VARCHAR:
            case OTHER:
                //hyperUnique, approxHistogram Druid column types
                jsonGen.writeString(VARCHAR.getSlice(block, position).toStringUtf8());
                return;
            case BIGINT:
            case TIMESTAMP:
                jsonGen.writeNumber(BIGINT.getLong(block, position));
                return;
            case FLOAT:
            case DOUBLE:
                jsonGen.writeNumber(DOUBLE.getDouble(block, position));
                return;
            default:
                throw new IllegalArgumentException("unsupported type: " + dataType);
        }
    }
}