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
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteOrder;

public class HDFSCompressedColumnarDoublesSuppliers
{
    public static final byte LZF_VERSION = 0x1;
    public static final byte VERSION = 0x2;

    private HDFSCompressedColumnarDoublesSuppliers()
    {
    }

    public static Supplier<ColumnarDoubles> fromByteBuffer(HDFSByteBuff buffer, ByteOrder order)
            throws IOException
    {
        byte versionFromBuffer = buffer.get();

        if (versionFromBuffer == LZF_VERSION || versionFromBuffer == VERSION) {
            final int totalSize = buffer.getInt();
            final int sizePer = buffer.getInt();
            CompressionStrategy compression = CompressionStrategy.LZF;
            if (versionFromBuffer == VERSION) {
                byte compressionId = buffer.get();
                compression = CompressionStrategy.forId(compressionId);
            }
            return HDFSCompressionFactory.getDoubleSupplier(
                    totalSize,
                    sizePer,
                    buffer.duplicate(),
                    order,
                    compression);
        }
        throw new IAE("Unknown version[%s]", versionFromBuffer);
    }
}
