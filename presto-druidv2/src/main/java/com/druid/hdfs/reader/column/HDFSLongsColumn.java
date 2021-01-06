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
package com.druid.hdfs.reader.column;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

public class HDFSLongsColumn
        implements NumericColumn
{
    /**
     * Factory method to create LongsColumn.
     */
    public static HDFSLongsColumn create(ColumnarLongs column, ImmutableBitmap nullValueBitmap)
    {
        if (nullValueBitmap.isEmpty()) {
            return new HDFSLongsColumn(column);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    final ColumnarLongs column;

    HDFSLongsColumn(final ColumnarLongs column)
    {
        this.column = column;
    }

    public ColumnarLongs getColumn()
    {
        return column;
    }

    @Override
    public int length()
    {
        return column.size();
    }

    @Override
    public long getLongSingleValueRow(int rowNum)
    {
        return column.get(rowNum);
    }

    @Override
    public void close()
    {
        column.close();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
        inspector.visit("column", column);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
    {
        return column.makeColumnValueSelector(offset, IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap());
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
    {
        return column.makeVectorValueSelector(offset, IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap());
    }
}
