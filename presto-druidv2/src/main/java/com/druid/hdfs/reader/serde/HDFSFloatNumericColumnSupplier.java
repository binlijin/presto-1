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

import com.druid.hdfs.reader.data.HDFSCompressedColumnarFloatsSupplier;
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.FloatsColumn;
import org.apache.druid.segment.column.NumericColumn;

public class HDFSFloatNumericColumnSupplier
        implements Supplier<NumericColumn>
{
    private final HDFSCompressedColumnarFloatsSupplier column;
    private final ImmutableBitmap nullValueBitmap;

    public HDFSFloatNumericColumnSupplier(HDFSCompressedColumnarFloatsSupplier column,
            ImmutableBitmap nullValueBitmap)
    {
        this.column = column;
        this.nullValueBitmap = nullValueBitmap;
    }

    @Override public NumericColumn get()
    {
        return FloatsColumn.create(column.get(), nullValueBitmap);
    }
}