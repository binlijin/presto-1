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
import com.google.common.base.Supplier;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.serde.DoubleNumericColumnPartSerde;

import java.io.IOException;
import java.nio.ByteOrder;

public class HDFSDoubleNumericColumnPartSerde
{
    private DoubleNumericColumnPartSerde doublePart;
    private final ByteOrder byteOrder;

    public HDFSDoubleNumericColumnPartSerde(DoubleNumericColumnPartSerde doublePart)
    {
        this.doublePart = doublePart;
        this.byteOrder = doublePart.getByteOrder();
    }

    void read(HDFSByteBuff buffer, ColumnBuilder builder, ColumnConfig columnConfig)
            throws IOException
    {
        final Supplier<ColumnarDoubles> column =
                HDFSCompressedColumnarDoublesSuppliers.fromByteBuffer(buffer, byteOrder);
        HDFSDoubleNumericColumnSupplier columnSupplier = new HDFSDoubleNumericColumnSupplier(
                column,
                IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap());
        builder.setType(ValueType.DOUBLE)
                .setHasMultipleValues(false)
                .setNumericColumnSupplier(columnSupplier);
    }
}
