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

import com.druid.hdfs.reader.column.HDFSStringDictionaryEncodedColumn;
import com.druid.hdfs.reader.data.HDFSCachingIndexed;
import com.druid.hdfs.reader.data.HDFSGenericIndexed;
import com.google.common.base.Supplier;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;

import javax.annotation.Nullable;

public class HDFSDictionaryEncodedColumnSupplier
        implements Supplier<DictionaryEncodedColumn<?>>
{
    private DictionaryEncodedColumn column;

    public HDFSDictionaryEncodedColumnSupplier(
            HDFSGenericIndexed<String> dictionary,
            @Nullable Supplier<ColumnarInts> singleValuedColumn,
            @Nullable Supplier<ColumnarMultiInts> multiValuedColumn,
            int lookupCacheSize)
    {
        this.column = new HDFSStringDictionaryEncodedColumn(
                singleValuedColumn != null ? singleValuedColumn.get() : null,
                multiValuedColumn != null ? multiValuedColumn.get() : null,
                new HDFSCachingIndexed<>(dictionary, lookupCacheSize));
    }

    @Override
    public DictionaryEncodedColumn<?> get()
    {
        return column;
    }
}
