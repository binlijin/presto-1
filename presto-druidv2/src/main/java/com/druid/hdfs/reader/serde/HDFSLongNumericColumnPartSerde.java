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
package com.druid.hdfs.reader.serde;

import com.druid.hdfs.reader.data.HDFSCompressedColumnarLongsSupplier;
import com.druid.hdfs.reader.utils.HDFSByteBuff;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.LongNumericColumnPartSerde;

import java.io.IOException;
import java.nio.ByteOrder;

public class HDFSLongNumericColumnPartSerde
{
    private LongNumericColumnPartSerde longPart;
    private final ByteOrder byteOrder;

    public HDFSLongNumericColumnPartSerde(LongNumericColumnPartSerde longPart)
    {
        this.longPart = longPart;
        this.byteOrder = longPart.getByteOrder();
    }

    public void read(HDFSByteBuff buffer, ColumnBuilder builder, ColumnConfig columnConfig)
            throws IOException
    {
        final HDFSCompressedColumnarLongsSupplier column =
                HDFSCompressedColumnarLongsSupplier.fromByteBuffer(buffer, byteOrder);
        HDFSLongNumericColumnSupplier columnSupplier = new HDFSLongNumericColumnSupplier(
                column,
                IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap());
        builder.setType(ValueType.LONG)
                .setHasMultipleValues(false)
                .setNumericColumnSupplier(columnSupplier);
    }
}
