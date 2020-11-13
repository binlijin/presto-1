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
package io.prestosql.druid;

import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class DruidFilterConverter
{
    private static final Logger LOG = Logger.get(DruidFilterConverter.class);

    static {
        NullHandling.initializeForTests();
    }

    private DruidFilterConverter()
    {
    }

    public static DimFilter generateFilter(DruidTableHandle tableHandle,
            List<DruidColumnHandle> columnHandles)
    {
        TupleDomain<ColumnHandle> tupleDomain = tableHandle.getConstraint();
        List<DimFilter> fields = new ArrayList<>();
        if (!tupleDomain.equals(TupleDomain.all())) {
            for (DruidColumnHandle columnHandle : columnHandles) {
                // skip druid __time field.
                if (columnHandle.getColumnName().equalsIgnoreCase("__time")) {
                    continue;
                }
                Domain domain = tupleDomain.getDomains().get().get(columnHandle);
                if (domain != null) {
                    DimFilter filter = toPredicate(columnHandle, domain);
                    if (filter != null) {
                        fields.add(filter);
                    }
                }
            }
        }
        if (fields.isEmpty()) {
            return null;
        }
        else {
            return new AndDimFilter(fields);
        }
    }

    private static DimFilter toPredicate(DruidColumnHandle columnHandle, Domain domain)
    {
        String columnName = columnHandle.getColumnName();
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        List<DimFilter> fields = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                Boolean lowerStrict = null;
                Boolean upperStrict = null;
                String lower = null;
                String upper = null;
                if (!range.getLow().isLowerUnbounded()) {
                    lower = singleQuote(range.getLow().getValue());
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toConjunct(columnName, ">", range.getLow().getValue()));
                            lowerStrict = true;
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toConjunct(columnName, ">=", range.getLow().getValue()));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    upper = singleQuote(range.getHigh().getValue());
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toConjunct(columnName, "<=", range.getHigh().getValue()));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toConjunct(columnName, "<", range.getHigh().getValue()));
                            upperStrict = true;
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which is not supported in pql
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                StringComparator ordering = getStringComparator(columnHandle.getColumnType());
                if (ordering != null) {
                    BoundDimFilter boundDimFilter =
                            new BoundDimFilter(columnHandle.getColumnName(), lower, upper,
                                    lowerStrict, upperStrict, null, null, ordering, null);
                    fields.add(boundDimFilter);
                    if (LOG.isDebugEnabled()) {
                        //LOG.debug("Add BoundDimFilter = " + boundDimFilter);
                    }
                }
            }
        }
        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            //disjuncts.add(toConjunct(columnName, "=", getOnlyElement(singleValues)));
            Object value = getOnlyElement(singleValues);
            SelectorDimFilter selectorDimFilter =
                    new SelectorDimFilter(columnName, singleQuote(value), null);
            return selectorDimFilter;
        }
        else if (singleValues.size() > 1) {
            //disjuncts.add(format("%s", inClauseValues(columnName, singleValues)));
            List<String> values = new ArrayList<>();
            for (Object value : singleValues) {
                values.add(singleQuote(value));
            }
            InDimFilter inDimFilter = new InDimFilter(columnName, values, null, null);
            return inDimFilter;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("disjuncts = " + disjuncts);
        }
        if (!fields.isEmpty()) {
            if (fields.size() > 1) {
                return new AndDimFilter(fields);
            }
            else {
                //LOG.debug("return filter = " + fields.get(0));
                return fields.get(0);
            }
        }
        return null;
    }

    private static String toConjunct(String columnName, String operator, Object value)
    {
        if (value instanceof Slice) {
            value = ((Slice) value).toStringUtf8();
        }
        return format("%s %s %s", columnName, operator, singleQuote(value));
    }

    private static String inClauseValues(String columnName, List<Object> singleValues)
    {
        return format("%s IN (%s)", columnName, singleValues.stream()
                .map(DruidFilterConverter::singleQuote)
                .collect(joining(", ")));
    }

    private static String singleQuote(Object value)
    {
        if (value instanceof Slice) {
            value = ((Slice) value).toStringUtf8();
        }
        return format("%s", value);
    }

    static StringComparator getStringComparator(Type type)
    {
        if (type == VARCHAR) {
            return StringComparators.LEXICOGRAPHIC;
        }
        if (type == DOUBLE) {
            return StringComparators.NUMERIC;
        }
        if (type == BIGINT) {
            return StringComparators.NUMERIC;
        }
        if (type == REAL) {
            return StringComparators.NUMERIC;
        }
        return null;
    }
}
