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
package io.prestosql.druid;

import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.prestosql.druid.TestingMetadataUtil.COLUMN_CODEC;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestDruidColumnHandle
{
    private final DruidColumnHandle columnHandle = new DruidColumnHandle("columnName", VARCHAR, DruidColumnHandle.DruidColumnType.REGULAR);

    @Test
    public void testJsonRoundTrip()
    {
        String json = COLUMN_CODEC.toJson(columnHandle);
        DruidColumnHandle copy = COLUMN_CODEC.fromJson(json);
        assertEquals(copy, columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester
                .equivalenceTester()
                .addEquivalentGroup(
                        new DruidColumnHandle("columnName", BIGINT, DruidColumnHandle.DruidColumnType.REGULAR),
                        new DruidColumnHandle("columnName", BIGINT, DruidColumnHandle.DruidColumnType.DERIVED))
                .addEquivalentGroup(
                        new DruidColumnHandle("columnNameX", VARCHAR, DruidColumnHandle.DruidColumnType.REGULAR),
                        new DruidColumnHandle("columnNameX", VARCHAR, DruidColumnHandle.DruidColumnType.DERIVED))
                .check();
    }
}
