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
package io.prestosql.plugin.cassandra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.schedule.NodeSelectionStrategy;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class CassandraSplit
        implements ConnectorSplit
{
    private final String partitionId;
    private final List<HostAddress> addresses;
    private final String splitCondition;

    @JsonCreator
    public CassandraSplit(
            @JsonProperty("partitionId") String partitionId,
            @JsonProperty("splitCondition") String splitCondition,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        requireNonNull(partitionId, "partitionName is null");
        requireNonNull(addresses, "addresses is null");

        this.partitionId = partitionId;
        this.addresses = ImmutableList.copyOf(addresses);
        this.splitCondition = splitCondition;
    }

    @JsonProperty
    public String getSplitCondition()
    {
        return splitCondition;
    }

    @JsonProperty
    public String getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return addresses;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("hosts", addresses)
                .put("partitionId", partitionId)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(partitionId)
                .toString();
    }

    public String getWhereClause()
    {
        if (partitionId.equals(CassandraPartition.UNPARTITIONED_ID)) {
            if (splitCondition != null) {
                return " WHERE " + splitCondition;
            }
            else {
                return "";
            }
        }
        else {
            if (splitCondition != null) {
                return " WHERE " + partitionId + " AND " + splitCondition;
            }
            else {
                return " WHERE " + partitionId;
            }
        }
    }
}
