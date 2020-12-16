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
package io.prestosql.plugin.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.schedule.NodeSelectionStrategy;

import java.net.URI;
import java.util.List;

import static io.prestosql.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class ExampleSplit
        implements ConnectorSplit
{
    private final URI uri;
    private final List<HostAddress> addresses;

    @JsonCreator
    public ExampleSplit(
            @JsonProperty("uri") URI uri)
    {
        this.uri = requireNonNull(uri, "uri is null");

        addresses = ImmutableList.of(HostAddress.fromUri(uri));
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        // only http or https is remotely accessible
        return NO_PREFERENCE;
    }

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
