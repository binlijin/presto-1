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
package com.facebook.presto.druid;

import com.facebook.presto.druid.ingestion.DruidIngestionTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

public class DruidHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return DruidTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return DruidColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return DruidSplit.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return DruidTransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return DruidIngestionTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return DruidIngestionTableHandle.class;
    }
}
