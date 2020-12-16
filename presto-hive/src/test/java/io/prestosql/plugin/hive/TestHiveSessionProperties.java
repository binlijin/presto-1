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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.schedule.NodeSelectionStrategy;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.HiveSessionProperties.getNodeSelectionStrategy;
import static io.prestosql.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static io.prestosql.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static org.testng.Assert.assertEquals;

public class TestHiveSessionProperties
{
    @Test
    public void testEmptyNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = HiveTestUtils.getHiveSession(new HiveConfig());
        assertEquals(getNodeSelectionStrategy(connectorSession), NO_PREFERENCE);
    }

    @Test
    public void testEmptyConfigNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = HiveTestUtils.getHiveSession(
                new HiveConfig().setNodeSelectionStrategy(NodeSelectionStrategy.valueOf("NO_PREFERENCE")));
        assertEquals(getNodeSelectionStrategy(connectorSession), NO_PREFERENCE);
    }

    @Test
    public void testNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = HiveTestUtils.getHiveSession(
                new HiveConfig().setNodeSelectionStrategy(HARD_AFFINITY));
        assertEquals(getNodeSelectionStrategy(connectorSession), HARD_AFFINITY);
    }
}
