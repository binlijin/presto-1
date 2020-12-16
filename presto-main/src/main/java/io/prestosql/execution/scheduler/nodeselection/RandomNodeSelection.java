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
package io.prestosql.execution.scheduler.nodeselection;

import com.google.common.collect.ImmutableSet;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.scheduler.NodeMap;
import io.prestosql.execution.scheduler.ResettableRandomizedIterator;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;

import java.util.List;

import static io.prestosql.execution.scheduler.NodeScheduler.randomizedNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectNodes;
import static java.util.Objects.requireNonNull;

public class RandomNodeSelection
        implements NodeSelection
{
    private final boolean includeCoordinator;
    private final int minCandidates;

    private ResettableRandomizedIterator<InternalNode> randomCandidates;

    public RandomNodeSelection(
            NodeMap nodeMap,
            boolean includeCoordinator,
            int minCandidates,
            List<RemoteTask> existingTasks)
    {
        requireNonNull(nodeMap, "nodeMap is null");
        requireNonNull(existingTasks, "existingTasks is null");

        this.includeCoordinator = includeCoordinator;
        this.minCandidates = minCandidates;
        this.randomCandidates = randomizedNodes(nodeMap, includeCoordinator, ImmutableSet.of());
    }

    @Override
    public List<InternalNode> pickNodes(Split split)
    {
        randomCandidates.reset();
        return selectNodes(minCandidates, randomCandidates);
    }
}
