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
package com.druid.hdfs.reader.data;

import com.druid.hdfs.reader.utils.HDFSByteBuff;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.serde.DoubleNumericColumnPartSerde;
import org.apache.druid.segment.serde.FloatNumericColumnPartSerde;
import org.apache.druid.segment.serde.LongNumericColumnPartSerde;

import java.io.IOException;

public class HDFSColumnDescriptor
{
    private static final Logger log = new Logger(HDFSColumnDescriptor.class);

    private ColumnDescriptor columnDescriptor;

    public HDFSColumnDescriptor(ColumnDescriptor columnDescriptor)
    {
        this.columnDescriptor = columnDescriptor;
    }

    public ColumnHolder read(HDFSByteBuff buffer, ColumnConfig columnConfig,
            SmooshedFileMapper smooshedFiles) throws IOException
    {
        final ColumnBuilder builder = new ColumnBuilder().setType(columnDescriptor.getValueType())
                .setHasMultipleValues(columnDescriptor.isHasMultipleValues())
                .setFileMapper(smooshedFiles);

        for (ColumnPartSerde part : columnDescriptor.getParts()) {
            //log.info("part = " + part);
            if (part instanceof LongNumericColumnPartSerde) {
                LongNumericColumnPartSerde longPart = (LongNumericColumnPartSerde) part;
                new HDFSLongNumericColumnPartSerde(longPart).read(buffer, builder, columnConfig);
            }
            else if (part instanceof DictionaryEncodedColumnPartSerde) {
                DictionaryEncodedColumnPartSerde dictPart = (DictionaryEncodedColumnPartSerde) part;
                new HDFSDictionaryEncodedColumnPartSerde(dictPart)
                        .read(buffer, builder, columnConfig);
            }
            else if (part instanceof DoubleNumericColumnPartSerde) {
                DoubleNumericColumnPartSerde doublePart = (DoubleNumericColumnPartSerde) part;
                new HDFSDoubleNumericColumnPartSerde(doublePart)
                        .read(buffer, builder, columnConfig);
            }
            else if (part instanceof FloatNumericColumnPartSerde) {
                FloatNumericColumnPartSerde floatPart = (FloatNumericColumnPartSerde) part;
                new HDFSFloatNumericColumnPartSerde(floatPart).read(buffer, builder, columnConfig);
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
        return builder.build();
    }
}
