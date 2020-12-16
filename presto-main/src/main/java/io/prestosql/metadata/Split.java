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
package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.Lifespan;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.SplitContext;
import io.prestosql.spi.schedule.NodeSelectionStrategy;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.spi.connector.SplitContext.NON_CACHEABLE;
import static java.util.Objects.requireNonNull;

public final class Split
{
    private final CatalogName catalogName;
    private final ConnectorSplit connectorSplit;
    private final Lifespan lifespan;
    private final SplitContext splitContext;

    // TODO: inline
    public Split(CatalogName catalogName, ConnectorSplit connectorSplit, Lifespan lifespan)
    {
        this(catalogName, connectorSplit, lifespan, NON_CACHEABLE);
    }

    @JsonCreator
    public Split(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit,
            @JsonProperty("lifespan") Lifespan lifespan,
            @JsonProperty("splitContext") SplitContext splitContext)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
        this.lifespan = requireNonNull(lifespan, "lifespan is null");
        this.splitContext = requireNonNull(splitContext, "splitContext is null");
    }

    @JsonProperty
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public ConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }

    @JsonProperty
    public Lifespan getLifespan()
    {
        return lifespan;
    }

    @JsonProperty
    public SplitContext getSplitContext()
    {
        return splitContext;
    }

    public Object getInfo()
    {
        return connectorSplit.getInfo();
    }

    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return connectorSplit.getPreferredNodes(sortedCandidates);
    }

    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return connectorSplit.getNodeSelectionStrategy();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("connectorSplit", connectorSplit)
                .add("lifespan", lifespan)
                .add("splitContext", splitContext)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Split split = (Split) o;
        return catalogName.equals(split.catalogName) &&
                connectorSplit.equals(split.connectorSplit) &&
                lifespan.equals(split.lifespan);
    }

    @Override
    public int hashCode()
    {
        // Requires connectorSplit's hash function to be set up correctly
        return Objects.hash(catalogName, connectorSplit, lifespan);
    }
}
