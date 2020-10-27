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

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.BitmapIndex;

import javax.annotation.Nullable;

public class HDFSBitmapIndexColumnPartSupplier
        implements Supplier<BitmapIndex>
{
    private final BitmapFactory bitmapFactory;
    private final HDFSGenericIndexed<ImmutableBitmap> bitmaps;
    private final HDFSGenericIndexed<String> dictionary;

    public HDFSBitmapIndexColumnPartSupplier(BitmapFactory bitmapFactory,
            HDFSGenericIndexed<ImmutableBitmap> bitmaps, HDFSGenericIndexed<String> dictionary)
    {
        this.bitmapFactory = bitmapFactory;
        this.bitmaps = bitmaps;
        this.dictionary = dictionary;
    }

    @Override public BitmapIndex get()
    {
        return new BitmapIndex() {
            @Override public int getCardinality()
            {
                return dictionary.size();
            }

            @Override public String getValue(int index)
            {
                return dictionary.get(index);
            }

            @Override public boolean hasNulls()
            {
                return dictionary.indexOf(null) >= 0;
            }

            @Override public BitmapFactory getBitmapFactory()
            {
                return bitmapFactory;
            }

            @Override public int getIndex(@Nullable String value)
            {
                // GenericIndexed.indexOf satisfies contract needed by BitmapIndex.indexOf
                return dictionary.indexOf(value);
            }

            @Override public ImmutableBitmap getBitmap(int idx)
            {
                if (idx < 0) {
                    return bitmapFactory.makeEmptyImmutableBitmap();
                }

                final ImmutableBitmap bitmap = bitmaps.get(idx);
                return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
            }
        };
    }
}
