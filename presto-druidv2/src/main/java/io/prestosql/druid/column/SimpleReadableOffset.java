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

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;

public class SimpleReadableOffset
        extends Offset
{
    private final int rowCount;
    private final int initialOffset;
    private int currentOffset;

    public SimpleReadableOffset(int rowCount)
    {
        this(0, rowCount);
    }

    private SimpleReadableOffset(int initialOffset, int rowCount)
    {
        this.initialOffset = initialOffset;
        this.currentOffset = initialOffset;
        this.rowCount = rowCount;
    }

    @Override
    public int getOffset()
    {
        return currentOffset;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public void increment()
    {
        currentOffset++;
    }

    @Override
    public boolean withinBounds()
    {
        return currentOffset < rowCount;
    }

    @Override
    public void reset()
    {
        currentOffset = initialOffset;
    }

    @Override
    public ReadableOffset getBaseReadableOffset()
    {
        return this;
    }
}
