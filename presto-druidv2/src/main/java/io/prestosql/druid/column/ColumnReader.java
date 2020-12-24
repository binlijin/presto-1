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

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.filter.SelectorFilter;

import static io.prestosql.druid.DruidErrorCode.DRUID_UNSUPPORTED_TYPE_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public interface ColumnReader
{
    Block readBlock(Type type, int batchSize, boolean filterBatch);

    default boolean hasPostFilter()
    {
        return false;
    }

    default boolean filterBatch()
    {
        return false;
    }

    static ColumnReader createColumnReader(Type type, ColumnValueSelector valueSelector, Offset offset, DimFilter postFilter)
    {
        if (type == VARCHAR) {
            return new StringColumnReader(offset, valueSelector);
        }
        if (type == DOUBLE) {
            return new DoubleColumnReader(offset, valueSelector);
        }
        if (type == BIGINT) {
            return new LongColumnReader(offset, valueSelector, postFilter);
        }
        if (type == REAL) {
            return new FloatColumnReader(offset, valueSelector);
        }
        if (type == TIMESTAMP) {
            return new TimestampColumnReader(offset, valueSelector);
        }
        throw new PrestoException(DRUID_UNSUPPORTED_TYPE_ERROR, format("Unsupported type: %s", type));
    }

    static ColumnReader createConstantsColumnReader(Type type, SelectorFilter selectorFilter)
    {
        if (type == VARCHAR) {
            return new ConstantStringColumnReader(selectorFilter);
        }
        throw new PrestoException(DRUID_UNSUPPORTED_TYPE_ERROR, format("Unsupported type: %s", type));
    }
}
