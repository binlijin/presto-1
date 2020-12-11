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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.druid.ingestion.DruidIngestionTableHandle;
import io.prestosql.druid.metadata.DruidColumnInfo;
import io.prestosql.druid.metadata.DruidColumnType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class DruidMetadata
        implements ConnectorMetadata
{
    private static final Object ALL_TABLES_CACHE_KEY = new Object();

    private final DruidClient druidClient;
    private final LoadingCache<Object, List<String>> allTablesCache;
    private final LoadingCache<String, List<DruidColumnInfo>> druidTableColumnCache;

    @Inject
    public DruidMetadata(DruidConfig config, DruidClient druidClient, @ForDruidClient Executor executor)
    {
        requireNonNull(config, "config is null");
        long metadataCacheExpiryMillis = config.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.allTablesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(druidClient::getAllTables), executor));
        this.druidTableColumnCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<>()
                        {
                            @Override
                            public List<DruidColumnInfo> load(String tableName)
                                    throws Exception
                            {
                                return druidClient.getColumnDataType(tableName);
                            }
                        }, executor));
        this.druidClient = requireNonNull(druidClient, "druidClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return druidClient.getSchemas();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getDruidTableNames().stream()
                .filter(name -> name.equals(tableName.getTableName()))
                .map(name -> DruidTableHandle.fromSchemaTableName(tableName))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        List<ColumnMetadata> columns = getDruidColumns(druidTable.getTableName()).stream()
                .map(column -> toColumnMetadata(column))
                .collect(toImmutableList());

        return new ConnectorTableMetadata(druidTable.toSchemaTableName(), columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return getDruidTableNames().stream()
                .map(tableName -> new SchemaTableName(druidClient.getSchema(), tableName))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTable = (DruidTableHandle) tableHandle;
        return getDruidColumns(druidTable.getTableName()).stream()
                .collect(toImmutableMap(DruidColumnInfo::getColumnName, column -> toColumnHandle(column)));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, DruidTableHandle
                    .fromSchemaTableName(tableName));
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DruidColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DruidTableHandle druidTableHandle = (DruidTableHandle) tableHandle;
        List<DruidColumnInfo> columns = getDruidColumns(druidTableHandle.getTableName());
        return new DruidIngestionTableHandle(druidTableHandle.getSchemaName(), druidTableHandle.getTableName(), columns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return new DruidIngestionTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableMetadata.getColumns().stream()
                        .map(column -> new DruidColumnInfo(column.getName(), DruidColumnType.fromPrestoType(column.getType())))
                        .collect(Collectors.toList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        DruidTableHandle handle = (DruidTableHandle) table;
        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new DruidTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint(),
                OptionalLong.of(limit));
        return Optional.of(new LimitApplicationResult<>(handle, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        DruidTableHandle handle = (DruidTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new DruidTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                newDomain,
                handle.getLimit());
        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public List<DruidColumnInfo> getDruidColumns(String tableName)
    {
        return getFromCache(druidTableColumnCache, tableName);
    }

    private List<String> getDruidTableNames()
    {
        return getFromCache(allTablesCache, ALL_TABLES_CACHE_KEY);
    }

    private static <K, V> V getFromCache(LoadingCache<K, V> cache, K key)
    {
        V value = cache.getIfPresent(key);
        if (value != null) {
            return value;
        }
        try {
            return cache.get(key);
        }
        catch (ExecutionException e) {
            throw new PrestoException(DruidErrorCode.DRUID_METADATA_ERROR, Optional.empty(), "Cannot fetch from cache " + key, e.getCause());
        }
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable() == null) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    private static ColumnMetadata toColumnMetadata(DruidColumnInfo column)
    {
        return new ColumnMetadata(column.getColumnName(), column.getDataType().getPrestoType());
    }

    private static ColumnHandle toColumnHandle(DruidColumnInfo column)
    {
        return new DruidColumnHandle(column.getColumnName(), column.getDataType().getPrestoType());
    }
}
