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
package io.prestosql.server.ui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.memory.ClusterMemoryManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.NodeState;
import io.prestosql.server.BasicQueryInfo;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/ui/api/stats")
public class ClusterStatsResource
{
    private final InternalNodeManager nodeManager;
    private final DispatchManager dispatchManager;
    private final boolean isIncludeCoordinator;
    private final ClusterMemoryManager clusterMemoryManager;

    @Inject
    public ClusterStatsResource(NodeSchedulerConfig nodeSchedulerConfig, InternalNodeManager nodeManager, DispatchManager dispatchManager, ClusterMemoryManager clusterMemoryManager)
    {
        this.isIncludeCoordinator = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.clusterMemoryManager = requireNonNull(clusterMemoryManager, "clusterMemoryManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterStats getClusterStats()
    {
        long runningQueries = 0;
        long blockedQueries = 0;
        long queuedQueries = 0;
        long activeNodes = nodeManager.getNodes(NodeState.ACTIVE).stream()
                .filter(node -> isIncludeCoordinator || !node.isCoordinator())
                .count();

        long activeCoordinators = nodeManager.getNodes(NodeState.ACTIVE).stream()
                .filter(node -> node.isCoordinator())
                .count();
        long totalAvailableProcessors = clusterMemoryManager.getTotalAvailableProcessors();

        long runningDrivers = 0;
        double memoryReservation = 0;

        long totalInputRows = dispatchManager.getStats().getConsumedInputRows().getTotalCount();
        long totalInputBytes = dispatchManager.getStats().getConsumedInputBytes().getTotalCount();
        long totalCpuTimeSecs = dispatchManager.getStats().getConsumedCpuTimeSecs().getTotalCount();

        for (BasicQueryInfo query : dispatchManager.getQueries()) {
            if (query.getState() == QueryState.QUEUED) {
                queuedQueries++;
            }
            else if (query.getState() == QueryState.RUNNING) {
                if (query.getQueryStats().isFullyBlocked()) {
                    blockedQueries++;
                }
                else {
                    runningQueries++;
                }
            }

            if (!query.getState().isDone()) {
                totalInputBytes += query.getQueryStats().getRawInputDataSize().toBytes();
                totalInputRows += query.getQueryStats().getRawInputPositions();
                totalCpuTimeSecs += query.getQueryStats().getTotalCpuTime().getValue(SECONDS);

                memoryReservation += query.getQueryStats().getUserMemoryReservation().toBytes();
                runningDrivers += query.getQueryStats().getRunningDrivers();
            }
        }

        return new ClusterStats(
                runningQueries,
                blockedQueries,
                queuedQueries,
                activeCoordinators,
                activeNodes,
                runningDrivers,
                totalAvailableProcessors,
                memoryReservation,
                totalInputRows,
                totalInputBytes,
                totalCpuTimeSecs);
    }

    @GET
    @Path("memory")
    public Response getClusterMemoryPoolInfo()
    {
        return Response.ok()
                .entity(clusterMemoryManager.getMemoryPoolInfo())
                .build();
    }

    @GET
    @Path("workerMemory")
    public Response getWorkerMemoryInfo()
    {
        return Response.ok()
                .entity(clusterMemoryManager.getWorkerMemoryInfo())
                .build();
    }

    public static class ClusterStats
    {
        private final long runningQueries;
        private final long blockedQueries;
        private final long queuedQueries;

        private final long activeCoordinators;
        private final long activeWorkers;
        private final long runningDrivers;

        private final long totalAvailableProcessors;

        private final double reservedMemory;

        private final long totalInputRows;
        private final long totalInputBytes;
        private final long totalCpuTimeSecs;

        @JsonCreator
        public ClusterStats(
                @JsonProperty("runningQueries") long runningQueries,
                @JsonProperty("blockedQueries") long blockedQueries,
                @JsonProperty("queuedQueries") long queuedQueries,
                @JsonProperty("activeCoordinators") long activeCoordinators,
                @JsonProperty("activeWorkers") long activeWorkers,
                @JsonProperty("runningDrivers") long runningDrivers,
                @JsonProperty("totalAvailableProcessors") long totalAvailableProcessors,
                @JsonProperty("reservedMemory") double reservedMemory,
                @JsonProperty("totalInputRows") long totalInputRows,
                @JsonProperty("totalInputBytes") long totalInputBytes,
                @JsonProperty("totalCpuTimeSecs") long totalCpuTimeSecs)
        {
            this.runningQueries = runningQueries;
            this.blockedQueries = blockedQueries;
            this.queuedQueries = queuedQueries;
            this.activeCoordinators = activeCoordinators;
            this.activeWorkers = activeWorkers;
            this.runningDrivers = runningDrivers;
            this.totalAvailableProcessors = totalAvailableProcessors;
            this.reservedMemory = reservedMemory;
            this.totalInputRows = totalInputRows;
            this.totalInputBytes = totalInputBytes;
            this.totalCpuTimeSecs = totalCpuTimeSecs;
        }

        @JsonProperty
        public long getRunningQueries()
        {
            return runningQueries;
        }

        @JsonProperty
        public long getBlockedQueries()
        {
            return blockedQueries;
        }

        @JsonProperty
        public long getQueuedQueries()
        {
            return queuedQueries;
        }

        @JsonProperty
        public long getActiveCoordinators()
        {
            return activeCoordinators;
        }

        @JsonProperty
        public long getActiveWorkers()
        {
            return activeWorkers;
        }

        @JsonProperty
        public long getRunningDrivers()
        {
            return runningDrivers;
        }

        @JsonProperty
        public long getTotalAvailableProcessors()
        {
            return totalAvailableProcessors;
        }

        @JsonProperty
        public double getReservedMemory()
        {
            return reservedMemory;
        }

        @JsonProperty
        public long getTotalInputRows()
        {
            return totalInputRows;
        }

        @JsonProperty
        public long getTotalInputBytes()
        {
            return totalInputBytes;
        }

        @JsonProperty
        public long getTotalCpuTimeSecs()
        {
            return totalCpuTimeSecs;
        }
    }
}