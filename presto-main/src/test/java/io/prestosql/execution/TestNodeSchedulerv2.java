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
package io.prestosql.execution;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.prestosql.client.NodeVersion;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.scheduler.NetworkLocation;
import io.prestosql.execution.scheduler.NetworkTopology;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.scheduler.SplitPlacementResult;
import io.prestosql.execution.scheduler.TopologyAwareNodeSelectorConfig;
import io.prestosql.execution.scheduler.UniformNodeSelectorFactory;
import io.prestosql.execution.scheduler.nodeselection.NodeSelector;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.schedule.NodeSelectionStrategy;
import io.prestosql.util.FinalizerService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static io.prestosql.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestNodeSchedulerv2
{
    private static final CatalogName CONNECTOR_ID = new CatalogName("connector_id");
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeSelector nodeSelector;
    private Map<InternalNode, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;
    private ScheduledExecutorService remoteTaskScheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerTask(10);

        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap));
        // contents of taskMap indicate the node-task map for the current stage
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(Optional.of(CONNECTOR_ID));
        remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));
        remoteTaskScheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("remoteTaskScheduledExecutor-%s"));

        finalizerService.start();
    }

    private void setUpNodes()
    {
        ImmutableList.Builder<InternalNode> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new InternalNode("other1", URI.create("http://10.0.0.1:11"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false));
        nodeBuilder.add(new InternalNode("other3", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false));
        ImmutableList<InternalNode> nodes = nodeBuilder.build();
        nodeManager.addNode(CONNECTOR_ID, nodes);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        remoteTaskExecutor.shutdown();
        remoteTaskScheduledExecutor.shutdown();
        finalizerService.destroy();
    }

    // Test exception throw when no nodes available to schedule
    @Test
    public void testAffinityAssignmentNotSupported()
    {
        InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:11"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, node1);
        InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, node2);
        InternalNode node3 = new InternalNode("node3", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, node3);

        Set<Split> splits = new HashSet<>();
        splits.add(new Split(CONNECTOR_ID, new TestSplitRemote(HostAddress.fromString("127.0.0.1:10")), Lifespan.taskWide()));
        splits.add(new Split(CONNECTOR_ID, new TestSplitRemote(HostAddress.fromString("127.0.0.1:10")), Lifespan.taskWide()));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        Set<InternalNode> internalNodes = splitPlacementResult.getAssignments().keySet();
        // Split doesn't support affinity schedule, fall back to random schedule
        assertEquals(internalNodes.size(), 2);
    }

    @Test
    public void testAffinityAssignment()
    {
        InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:11"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, node1);
        InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, node2);
        InternalNode node3 = new InternalNode("node3", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, node3);

        Set<Split> splits = new HashSet<>();

        // Adding one more split (1 % 3 = 1), 1 splits will be distributed to 1 nodes
        splits.add(new Split(CONNECTOR_ID, new TestAffinitySplitRemote(1), Lifespan.taskWide()));
        SplitPlacementResult splitPlacementResult = nodeSelector.computeAssignments(splits, ImmutableList.of());
        Set<InternalNode> internalNodes = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodes.size(), 1);

        // Adding one more split (2 % 3 = 2), 2 splits will be distributed to 2 nodes
        splits.add(new Split(CONNECTOR_ID, new TestAffinitySplitRemote(2), Lifespan.taskWide()));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        Set<InternalNode> internalNodesSecondCall = splitPlacementResult.getAssignments().keySet();
        assertEquals(internalNodesSecondCall.size(), 2);

        // adding one more split(4 % 3 = 1) that will fall into the same slots, 3 splits will be distributed to 2 nodes still
        splits.add(new Split(CONNECTOR_ID, new TestAffinitySplitRemote(4), Lifespan.taskWide()));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 2);

        // adding one more split(3 % 3 = 0) that will fall into different slots, 3 splits will be distributed to 3 nodes
        splits.add(new Split(CONNECTOR_ID, new TestAffinitySplitRemote(3), Lifespan.taskWide()));
        splitPlacementResult = nodeSelector.computeAssignments(splits, getRemoteTableScanTask(splitPlacementResult));
        assertEquals(splitPlacementResult.getAssignments().keySet().size(), 3);
    }

    private List<RemoteTask> getRemoteTableScanTask(SplitPlacementResult splitPlacementResult)
    {
        Map<InternalNode, RemoteTask> taskMap = new HashMap<>();
        Multimap<InternalNode, Split> assignments = splitPlacementResult.getAssignments();
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor, remoteTaskScheduledExecutor);
        int task = 0;
        for (InternalNode node : assignments.keySet()) {
            TaskId taskId = new TaskId("test", 1, task);
            task++;
            MockRemoteTaskFactory.MockRemoteTask remoteTask = remoteTaskFactory.createTableScanTask(taskId, node, ImmutableList.copyOf(assignments.get(node)), nodeTaskMap.createPartitionedSplitCountTracker(node, taskId));
            remoteTask.startSplits(25);
            nodeTaskMap.addTask(node, remoteTask);
            taskMap.put(node, remoteTask);
        }
        return ImmutableList.copyOf(taskMap.values());
    }

    private static class TestSplitLocal
            implements ConnectorSplit
    {
        private final HostAddress address;

        private TestSplitLocal()
        {
            this(HostAddress.fromString("10.0.0.1:11"));
        }

        private TestSplitLocal(HostAddress address)
        {
            this.address = requireNonNull(address, "address is null");
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of(address);
        }

        @Override
        public Object getInfo()
        {
            return this;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("address", address)
                    .toString();
        }
    }

    private static class TestSplitLocallyAccessible
            implements ConnectorSplit
    {
        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of(HostAddress.fromString("10.0.0.1:11"));
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }

    private static class TestSplitRemote
            implements ConnectorSplit
    {
        private final List<HostAddress> hosts;

        TestSplitRemote()
        {
            this(HostAddress.fromString(format("10.%s.%s.%s:%s",
                    ThreadLocalRandom.current().nextInt(0, 255),
                    ThreadLocalRandom.current().nextInt(0, 255),
                    ThreadLocalRandom.current().nextInt(0, 255),
                    ThreadLocalRandom.current().nextInt(15, 5000))));
        }

        TestSplitRemote(HostAddress host)
        {
            this.hosts = ImmutableList.of(requireNonNull(host, "host is null"));
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return hosts;
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }

    private static class TestAffinitySplitRemote
            extends TestSplitRemote
    {
        private int scheduleIdentifierId;

        public TestAffinitySplitRemote(int scheduleIdentifierId)
        {
            super();
            this.scheduleIdentifierId = scheduleIdentifierId;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NodeSelectionStrategy.SOFT_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of(sortedCandidates.get(scheduleIdentifierId % sortedCandidates.size()));
        }
    }

    private static class TestNetworkTopology
            implements NetworkTopology
    {
        @Override
        public NetworkLocation locate(HostAddress address)
        {
            List<String> parts = new ArrayList<>(ImmutableList.copyOf(Splitter.on(".").split(address.getHostText())));
            Collections.reverse(parts);
            return new NetworkLocation(parts);
        }
    }

    private static TopologyAwareNodeSelectorConfig getNetworkTopologyConfig()
    {
        return new TopologyAwareNodeSelectorConfig()
                .setLocationSegmentNames(ImmutableList.of("rack", "machine"));
    }
}
