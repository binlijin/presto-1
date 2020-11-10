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
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.WritableSupplier;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;

import java.io.IOException;
import java.nio.ByteOrder;

public class HDFSDictionaryEncodedColumnPartSerde
{
    private static final int NO_FLAGS = 0;

    private DictionaryEncodedColumnPartSerde dictPart;
    private final ByteOrder byteOrder;
    private final BitmapSerdeFactory bitmapSerdeFactory;

    public HDFSDictionaryEncodedColumnPartSerde(DictionaryEncodedColumnPartSerde dictPart)
    {
        this.dictPart = dictPart;
        this.byteOrder = dictPart.getByteOrder();
        this.bitmapSerdeFactory = dictPart.getBitmapSerdeFactory();
    }

    void read(HDFSByteBuff buffer, ColumnBuilder builder, ColumnConfig columnConfig)
            throws IOException
    {
        int rVersion = buffer.get();
        final int rFlags;

        if (rVersion - 2 >= 0) {
            rFlags = buffer.getInt();
        }
        else {
            rFlags = (rVersion == 1) ? 1 : NO_FLAGS;
        }

        final boolean hasMultipleValues = isSet(1, rFlags) || isSet(2, rFlags);

        final HDFSGenericIndexed<String> rDictionary =
                HDFSGenericIndexed.read(buffer, GenericIndexed.STRING_STRATEGY, true);
        builder.setType(ValueType.STRING);

        final WritableSupplier<ColumnarInts> rSingleValuedColumn;
        final WritableSupplier<ColumnarMultiInts> rMultiValuedColumn;

        if (hasMultipleValues) {
            throw new UnsupportedOperationException();
        }
        else {
            rSingleValuedColumn = readSingleValuedColumn(rVersion, buffer);
            rMultiValuedColumn = null;
        }

        HDFSDictionaryEncodedColumnSupplier dictionaryEncodedColumnSupplier =
                new HDFSDictionaryEncodedColumnSupplier(rDictionary, rSingleValuedColumn,
                        rMultiValuedColumn, columnConfig.columnCacheSizeBytes());
        builder.setHasMultipleValues(hasMultipleValues)
                .setDictionaryEncodedColumnSupplier(dictionaryEncodedColumnSupplier);

        if (!isSet(4, rFlags)) {
            HDFSGenericIndexed<ImmutableBitmap> rBitmaps =
                    HDFSGenericIndexed.read(buffer, bitmapSerdeFactory.getObjectStrategy());
            builder.setBitmapIndex(
                    new HDFSBitmapIndexColumnPartSupplier(bitmapSerdeFactory.getBitmapFactory(),
                            rBitmaps, rDictionary));
        }

        if (buffer.hasRemaining()) {
            throw new UnsupportedOperationException();
        }
    }

    private WritableSupplier<ColumnarInts> readSingleValuedColumn(int version, HDFSByteBuff buffer)
            throws IOException
    {
        switch (version) {
            case 0:
            case 1:
                throw new IAE("Unsupported single-value version[%s]", version);
                //return VSizeColumnarInts.readFromByteBuffer(buffer);
            case 2:
                return HDFSCompressedVSizeColumnarIntsSupplier.fromByteBuffer(buffer, byteOrder);
            default:
                throw new IAE("Unsupported single-value version[%s]", version);
        }
    }

    public boolean isSet(int mask, int flags)
    {
        return (mask & flags) != 0;
    }
}
