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

import com.google.common.collect.ImmutableList;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.druid.ingestion.DruidPageSinkProvider;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.druid.DruidTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class DruidConnector
        implements Connector
{
    private static final Logger log = Logger.get(DruidConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final DruidMetadata metadata;
    private final DruidSplitManager splitManager;
    private final DruidPageSourceProvider pageSourceProvider;
    private final DruidPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final DruidCachingFileSystem druidCachingFileSystem;

    @Inject
    public DruidConnector(
            LifeCycleManager lifeCycleManager,
            DruidMetadata metadata,
            DruidSplitManager splitManager,
            DruidPageSourceProvider pageSourceProvider,
            DruidPageSinkProvider pageSinkProvider,
            DruidSessionProperties druidSessionProperties,
            DruidCachingFileSystem druidCachingFileSystem)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvide is null");
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(druidSessionProperties, "sessionProperties is null").getSessionProperties());
        this.druidCachingFileSystem = requireNonNull(druidCachingFileSystem, "druidCachingFileSystem is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public final void shutdown()
    {
        try {
            druidCachingFileSystem.shutdown();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down DruidCachingFileSystem");
        }
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
