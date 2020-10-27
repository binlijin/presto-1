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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.HistoricalDimensionSelector;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.BitSet;

public class HDFSStringDictionaryEncodedColumn
        implements DictionaryEncodedColumn<String>
{
    @Nullable
    private final ColumnarInts column;
    @Nullable
    private final ColumnarMultiInts multiValueColumn;
    private final HDFSCachingIndexed<String> cachedLookups;

    public HDFSStringDictionaryEncodedColumn(@Nullable ColumnarInts singleValueColumn,
            @Nullable ColumnarMultiInts multiValueColumn,
            HDFSCachingIndexed<String> cachedLookups)
    {
        this.column = singleValueColumn;
        this.multiValueColumn = multiValueColumn;
        this.cachedLookups = cachedLookups;
    }

    @Override public int length()
    {
        return hasMultipleValues() ? multiValueColumn.size() : column.size();
    }

    @Override public boolean hasMultipleValues()
    {
        return column == null;
    }

    @Override public int getSingleValueRow(int rowNum)
    {
        return column.get(rowNum);
    }

    @Override public IndexedInts getMultiValueRow(int rowNum)
    {
        return multiValueColumn.get(rowNum);
    }

    @Override @Nullable public String lookupName(int id)
    {
        return cachedLookups.get(id);
    }

    @Override public int lookupId(String name)
    {
        return cachedLookups.indexOf(name);
    }

    @Override public int getCardinality()
    {
        return cachedLookups.size();
    }

    @Override public HistoricalDimensionSelector makeDimensionSelector(final ReadableOffset offset,
            @Nullable final ExtractionFn extractionFn)
    {
        abstract class QueryableDimensionSelector
                extends AbstractDimensionSelector
                implements HistoricalDimensionSelector, IdLookup
        {
            @Override public int getValueCardinality()
            {
        /*
         This is technically wrong if
         extractionFn != null && (extractionFn.getExtractionType() != ExtractionFn.ExtractionType.ONE_TO_ONE ||
                                    !extractionFn.preservesOrdering())
         However current behavior allows some GroupBy-V1 queries to work that wouldn't work otherwise and doesn't
         cause any problems due to special handling of extractionFn everywhere.
         See https://github.com/apache/druid/pull/8433
         */
                return getCardinality();
            }

            @Override public String lookupName(int id)
            {
                final String value = HDFSStringDictionaryEncodedColumn.this.lookupName(id);
                return extractionFn == null ? value : extractionFn.apply(value);
            }

            @Override public boolean nameLookupPossibleInAdvance()
            {
                return true;
            }

            @Nullable @Override public IdLookup idLookup()
            {
                return extractionFn == null ? this : null;
            }

            @Override public int lookupId(String name)
            {
                if (extractionFn != null) {
                    throw new UnsupportedOperationException(
                            "cannot perform lookup when applying an extraction function");
                }
                return HDFSStringDictionaryEncodedColumn.this.lookupId(name);
            }
        }

        if (hasMultipleValues()) {
            throw new UnsupportedOperationException();
        }
        else {
            class SingleValueQueryableDimensionSelector
                    extends QueryableDimensionSelector
                    implements SingleValueHistoricalDimensionSelector
            {
                private final SingleIndexedInt row = new SingleIndexedInt();

                @Override public IndexedInts getRow()
                {
                    row.setValue(getRowValue());
                    return row;
                }

                public int getRowValue()
                {
                    return column.get(offset.getOffset());
                }

                @Override public IndexedInts getRow(int offset)
                {
                    row.setValue(getRowValue(offset));
                    return row;
                }

                @Override public int getRowValue(int offset)
                {
                    return column.get(offset);
                }

                @Override public ValueMatcher makeValueMatcher(final @Nullable String value)
                {
                    if (extractionFn == null) {
                        final int valueId = lookupId(value);
                        if (valueId >= 0) {
                            return new ValueMatcher() {
                                @Override public boolean matches()
                                {
                                    return getRowValue() == valueId;
                                }

                                @Override
                                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                                {
                                    inspector.visit("column",
                                            HDFSStringDictionaryEncodedColumn.this);
                                }
                            };
                        }
                        else {
                            return BooleanValueMatcher.of(false);
                        }
                    }
                    else {
                        // Employ caching BitSet optimization
                        return makeValueMatcher(Predicates.equalTo(value));
                    }
                }

                @Override public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
                {
                    final BitSet checkedIds = new BitSet(getCardinality());
                    final BitSet matchingIds = new BitSet(getCardinality());

                    // Lazy matcher; only check an id if matches() is called.
                    return new ValueMatcher()
                    {
                        @Override public boolean matches()
                        {
                            final int id = getRowValue();

                            if (checkedIds.get(id)) {
                                return matchingIds.get(id);
                            }
                            else {
                                final boolean matches = predicate.apply(lookupName(id));
                                checkedIds.set(id);
                                if (matches) {
                                    matchingIds.set(id);
                                }
                                return matches;
                            }
                        }

                        @Override public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                        {
                            inspector.visit("column", HDFSStringDictionaryEncodedColumn.this);
                        }
                    };
                }

                @Override public Object getObject()
                {
                    return lookupName(getRowValue());
                }

                @Override public Class classOfObject()
                {
                    return String.class;
                }

                @Override public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {
                    inspector.visit("column", column);
                    inspector.visit("offset", offset);
                    inspector.visit("extractionFn", extractionFn);
                }
            }

            return new SingleValueQueryableDimensionSelector();
        }
    }

    @Override public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
            final ReadableVectorOffset offset)
    {
        class QueryableSingleValueDimensionVectorSelector
                implements SingleValueDimensionVectorSelector, IdLookup
        {
            private final int[] vector = new int[offset.getMaxVectorSize()];
            private int id = ReadableVectorOffset.NULL_ID;

            @Override public int[] getRowVector()
            {
                if (id == offset.getId()) {
                    return vector;
                }

                if (offset.isContiguous()) {
                    column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
                }
                else {
                    column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
                }

                id = offset.getId();
                return vector;
            }

            @Override public int getValueCardinality()
            {
                return getCardinality();
            }

            @Nullable @Override public String lookupName(final int id)
            {
                return HDFSStringDictionaryEncodedColumn.this.lookupName(id);
            }

            @Override public boolean nameLookupPossibleInAdvance()
            {
                return true;
            }

            @Nullable @Override public IdLookup idLookup()
            {
                return this;
            }

            @Override public int lookupId(@Nullable final String name)
            {
                return HDFSStringDictionaryEncodedColumn.this.lookupId(name);
            }

            @Override public int getCurrentVectorSize()
            {
                return offset.getCurrentVectorSize();
            }

            @Override public int getMaxVectorSize()
            {
                return offset.getMaxVectorSize();
            }
        }

        return new QueryableSingleValueDimensionVectorSelector();
    }

    @Override public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(
            final ReadableVectorOffset offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override public void close() throws IOException
    {
        CloseQuietly.close(cachedLookups);

        if (column != null) {
            column.close();
        }
        if (multiValueColumn != null) {
            multiValueColumn.close();
        }
    }
}
