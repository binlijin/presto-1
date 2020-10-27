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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.data.ColumnarFloats;
import org.apache.druid.segment.data.CompressedColumnarFloatsSupplier;
import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteOrder;

public class HDFSCompressedColumnarFloatsSupplier
        implements Supplier<ColumnarFloats>
{
    private final int totalSize;
    private final int sizePer;
    private final HDFSByteBuff buffer;
    private final Supplier<ColumnarFloats> supplier;
    private final CompressionStrategy compression;

    HDFSCompressedColumnarFloatsSupplier(
            int totalSize,
            int sizePer,
            HDFSByteBuff buffer,
            Supplier<ColumnarFloats> supplier,
            CompressionStrategy compression)
    {
        this.totalSize = totalSize;
        this.sizePer = sizePer;
        this.buffer = buffer;
        this.supplier = supplier;
        this.compression = compression;
    }

    @Override public ColumnarFloats get()
    {
        return supplier.get();
    }

    public static HDFSCompressedColumnarFloatsSupplier fromByteBuffer(HDFSByteBuff buffer,
            ByteOrder order) throws IOException
    {
        byte versionFromBuffer = buffer.get();

        if (versionFromBuffer == CompressedColumnarFloatsSupplier.LZF_VERSION
                || versionFromBuffer == CompressedColumnarFloatsSupplier.VERSION) {
            final int totalSize = buffer.getInt();
            final int sizePer = buffer.getInt();
            CompressionStrategy compression = CompressionStrategy.LZF;
            if (versionFromBuffer == CompressedColumnarFloatsSupplier.VERSION) {
                byte compressionId = buffer.get();
                compression = CompressionStrategy.forId(compressionId);
            }
            Supplier<ColumnarFloats> supplier = HDFSCompressionFactory
                    .getFloatSupplier(totalSize, sizePer, buffer.duplicate(), order, compression);
            return new HDFSCompressedColumnarFloatsSupplier(totalSize, sizePer, buffer, supplier,
                    compression);
        }

        throw new IAE("Unknown version[%s]", versionFromBuffer);
    }
}
