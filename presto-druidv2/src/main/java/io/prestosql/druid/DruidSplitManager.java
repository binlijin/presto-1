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
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.druid.DruidSplit.createBrokerSplit;
import static io.prestosql.druid.DruidSplit.createSegmentSplit;
import static java.util.Objects.requireNonNull;

public class DruidSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(DruidSplitManager.class);

    private final DruidClient druidClient;

    @Inject
    public DruidSplitManager(DruidClient druidClient)
    {
        this.druidClient = requireNonNull(druidClient, "druid client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingContext,
            DynamicFilter dynamicFilter)
    {
        DruidTableHandle table = (DruidTableHandle) tableHandle;
        //if (isComputePushdownEnabled(session) || (table.getDql().isPresent() && table.getDql().get().getPushdown())) {
        // TODO
        if (DruidSessionProperties.isComputePushdownEnabled(session)) {
            //table.getDql().get()
            return new FixedSplitSource(ImmutableList.of(createBrokerSplit()));
        }
        List<String> segmentIds = druidClient.getDataSegmentId(table.getTableName());
        // TODO according to timestamp filter segments
        // TODO 根据时间范围过滤segments
        List<DruidSplit> splits = segmentIds.stream()
                .map(id -> druidClient.getSingleSegmentInfo(table.getTableName(), id))
                .map(info -> createSegmentSplit(info, HostAddress.fromUri(druidClient.getDruidBroker())))
                .collect(toImmutableList());
        if (LOG.isDebugEnabled()) {
            LOG.debug("splits = " + splits);
        }
        return new FixedSplitSource(splits);
    }
}