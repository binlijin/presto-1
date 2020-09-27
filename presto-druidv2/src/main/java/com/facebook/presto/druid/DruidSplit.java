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

import com.facebook.presto.druid.metadata.DruidSegmentInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DruidSplit
        implements ConnectorSplit
{
    private final SplitType splitType;
    private final Optional<DruidSegmentInfo> segmentInfo;
    private final Optional<HostAddress> address;

    @JsonCreator
    public DruidSplit(
            @JsonProperty("splitType") SplitType splitType,
            @JsonProperty("segmentInfo") Optional<DruidSegmentInfo> segmentInfo,
            @JsonProperty("address") Optional<HostAddress> address)
    {
        this.splitType = requireNonNull(splitType, "splitType id is null");
        this.segmentInfo = requireNonNull(segmentInfo, "segment info is null");
        this.address = requireNonNull(address, "address info is null");
        if (splitType == SplitType.SEGMENT) {
            checkArgument(segmentInfo.isPresent(), "SegmentInfo is missing from split");
            checkArgument(address.isPresent(), "Address is missing from split");
        }
    }

    public static DruidSplit createBrokerSplit()
    {
        return new DruidSplit(
                SplitType.BROKER,
                Optional.empty(),
                Optional.empty());
    }

    public static DruidSplit createSegmentSplit(DruidSegmentInfo segmentInfo, HostAddress address)
    {
        return new DruidSplit(
                SplitType.SEGMENT,
                Optional.of(requireNonNull(segmentInfo, "segmentInfo are null")),
                Optional.of(requireNonNull(address, "address is null")));
    }

    @JsonProperty
    public SplitType getSplitType()
    {
        return splitType;
    }

    @JsonProperty
    public Optional<DruidSegmentInfo> getSegmentInfo()
    {
        return segmentInfo;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address.isPresent() ? address.get() : null;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splitType", splitType)
                .add("segmentInfo", segmentInfo)
                .add("address", address)
                .toString();
    }

    public enum SplitType
    {
        SEGMENT,
        BROKER,
    }
}
