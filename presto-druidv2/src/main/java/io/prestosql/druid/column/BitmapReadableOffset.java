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
package io.prestosql.druid.column;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;
import org.roaringbitmap.IntIterator;

public class BitmapReadableOffset
        extends Offset
{
    private static final int INVALID_VALUE = -1;

    private ImmutableBitmap filterBitmap;
    private IntIterator iterator;
    private int value;

    public BitmapReadableOffset(ImmutableBitmap filterBitmap)
    {
        this.filterBitmap = filterBitmap;
        this.iterator = filterBitmap.iterator();
        increment();
    }

    @Override
    public int getOffset()
    {
        return value;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public void increment()
    {
        if (iterator.hasNext()) {
            value = iterator.next();
        }
        else {
            value = INVALID_VALUE;
        }
    }

    @Override
    public boolean withinBounds()
    {
        return value > INVALID_VALUE;
    }

    @Override
    public void reset()
    {
        this.iterator = filterBitmap.iterator();
    }

    @Override
    public ReadableOffset getBaseReadableOffset()
    {
        return this;
    }
}
