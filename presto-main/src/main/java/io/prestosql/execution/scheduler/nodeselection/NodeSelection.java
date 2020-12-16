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

import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;

import java.util.List;

public interface NodeSelection
{
    /**
     *
     * Pick nodes according to different strategies for a split
     * @param split
     * @return picked nodes
     */
    List<InternalNode> pickNodes(Split split);
}
