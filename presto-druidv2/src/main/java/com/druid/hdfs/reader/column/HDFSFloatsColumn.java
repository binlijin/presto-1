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
import org.apache.druid.segment.data.ColumnarFloats;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

public class HDFSFloatsColumn
        implements NumericColumn
{
    /**
     * Factory method to create FloatsColumn.
     */
    public static HDFSFloatsColumn create(ColumnarFloats column, ImmutableBitmap nullValueBitmap)
    {
        if (nullValueBitmap.isEmpty()) {
            return new HDFSFloatsColumn(column);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    final ColumnarFloats column;

    HDFSFloatsColumn(final ColumnarFloats column)
    {
        this.column = column;
    }

    public ColumnarFloats getColumn()
    {
        return column;
    }

    @Override
    public int length()
    {
        return column.size();
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

    @Override
    public long getLongSingleValueRow(int rowNum)
    {
        return (long) column.get(rowNum);
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
}
