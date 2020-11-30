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

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.DoublesColumn;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ColumnarDoubles;

public class HDFSDoubleNumericColumnSupplier
        implements Supplier<NumericColumn>
{
    private NumericColumn column;

    public HDFSDoubleNumericColumnSupplier(Supplier<ColumnarDoubles> column,
            ImmutableBitmap nullValueBitmap)
    {
        this.column = DoublesColumn.create(column.get(), nullValueBitmap);
    }

    @Override
    public NumericColumn get()
    {
        return column;
    }
}
