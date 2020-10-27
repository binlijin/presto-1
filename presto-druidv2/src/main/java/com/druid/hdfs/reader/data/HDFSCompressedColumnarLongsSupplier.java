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
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteOrder;

public class HDFSCompressedColumnarLongsSupplier
        implements Supplier<ColumnarLongs>
{
    private final int totalSize;
    private final int sizePer;
    private final HDFSByteBuff buffer;
    private final Supplier<ColumnarLongs> supplier;
    private final CompressionStrategy compression;
    private final CompressionFactory.LongEncodingFormat encoding;

    public HDFSCompressedColumnarLongsSupplier(
            int totalSize,
            int sizePer,
            HDFSByteBuff buffer,
            Supplier<ColumnarLongs> supplier,
            CompressionStrategy compression,
            CompressionFactory.LongEncodingFormat encoding)
    {
        this.totalSize = totalSize;
        this.sizePer = sizePer;
        this.buffer = buffer;
        this.supplier = supplier;
        this.compression = compression;
        this.encoding = encoding;
    }

    @Override public ColumnarLongs get()
    {
        return supplier.get();
    }

    public static HDFSCompressedColumnarLongsSupplier fromByteBuffer(HDFSByteBuff buffer,
            ByteOrder order) throws IOException
    {
        byte versionFromBuffer = buffer.get();

        if (versionFromBuffer == CompressedColumnarLongsSupplier.LZF_VERSION
                || versionFromBuffer == CompressedColumnarLongsSupplier.VERSION) {
            final int totalSize = buffer.getInt();
            final int sizePer = buffer.getInt();
            CompressionStrategy compression = CompressionStrategy.LZF;
            CompressionFactory.LongEncodingFormat encoding =
                    CompressionFactory.LEGACY_LONG_ENCODING_FORMAT;
            if (versionFromBuffer == CompressedColumnarLongsSupplier.VERSION) {
                byte compressionId = buffer.get();
                if (CompressionFactory.hasEncodingFlag(compressionId)) {
                    encoding = CompressionFactory.LongEncodingFormat.forId(buffer.get());
                    compressionId = CompressionFactory.clearEncodingFlag(compressionId);
                }
                compression = CompressionStrategy.forId(compressionId);
            }
            if (encoding != CompressionFactory.LEGACY_LONG_ENCODING_FORMAT) {
                throw new UnsupportedOperationException();
            }
            CompressionFactory.LongEncodingReader reader = new LongsLongEncodingReader();
            Supplier<ColumnarLongs> supplier = HDFSCompressionFactory
                    .getLongSupplier(totalSize, sizePer, buffer.duplicate(), order, reader,
                            compression);
            return new HDFSCompressedColumnarLongsSupplier(totalSize, sizePer, buffer, supplier,
                    compression, encoding);
        }

        throw new IAE("Unknown version[%s]", versionFromBuffer);
    }
}
