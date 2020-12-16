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
package io.prestosql.connector.informationschema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.schedule.NodeSelectionStrategy;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static java.util.Objects.requireNonNull;

public class InformationSchemaSplit
        implements ConnectorSplit
{
    private final List<HostAddress> addresses;

    @JsonCreator
    public InformationSchemaSplit(
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        requireNonNull(addresses, "hosts is null");
        checkArgument(!addresses.isEmpty(), "hosts is empty");
        this.addresses = ImmutableList.copyOf(addresses);
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return HARD_AFFINITY;
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
    public Object getInfo()
    {
        return this;
    }
}
