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
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.ColumnarFloats;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteOrder;

public class HDFSCompressionFactory
{
    private HDFSCompressionFactory()
    {
    }

    public static Supplier<ColumnarLongs> getLongSupplier(int totalSize, int sizePer,
            HDFSByteBuff fromBuffer, ByteOrder order, CompressionFactory.LongEncodingReader reader,
            CompressionStrategy strategy) throws IOException
    {
        if (strategy == CompressionStrategy.NONE) {
            throw new UnsupportedOperationException();
        }
        else {
            return new HDFSBlockLayoutColumnarLongsSupplier(totalSize, sizePer, fromBuffer, order,
                    reader, strategy);
        }
    }

    // Float currently does not support any encoding types, and stores values as 4 byte float
    public static Supplier<ColumnarFloats> getFloatSupplier(int totalSize, int sizePer,
            HDFSByteBuff fromBuffer, ByteOrder order, CompressionStrategy strategy)
            throws IOException
    {
        if (strategy == CompressionStrategy.NONE) {
            throw new UnsupportedOperationException();
        }
        else {
            return new HDFSBlockLayoutColumnarFloatsSupplier(totalSize, sizePer, fromBuffer, order,
                    strategy);
        }
    }

    public static Supplier<ColumnarDoubles> getDoubleSupplier(int totalSize, int sizePer,
            HDFSByteBuff fromBuffer, ByteOrder byteOrder, CompressionStrategy strategy)
            throws IOException
    {
        switch (strategy) {
            case NONE:
                throw new UnsupportedOperationException();
            default:
                return new HDFSBlockLayoutColumnarDoublesSupplier(totalSize, sizePer, fromBuffer,
                        byteOrder, strategy);
        }
    }
}
