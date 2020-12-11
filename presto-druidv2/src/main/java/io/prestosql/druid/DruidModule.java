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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.druid.ingestion.DruidPageSinkProvider;
import io.prestosql.druid.ingestion.DruidPageWriter;

import java.util.concurrent.Executor;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DruidModule
        implements Module
{
    private final String catalogName;

    public DruidModule(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DruidConfig.class);
        binder.bind(DruidConnector.class).in(Scopes.SINGLETON);
        binder.bind(DruidMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DruidHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(DruidClient.class).in(Scopes.SINGLETON);
        //binder.bind(DruidPlanOptimizer.class).in(Scopes.SINGLETON);
        binder.bind(DruidSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DruidPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(DruidPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(DruidPageWriter.class).in(Scopes.SINGLETON);
        //binder.bind(DruidQueryGenerator.class).in(Scopes.SINGLETON);
        binder.bind(DruidSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(DruidCachingFileSystem.class).in(Scopes.SINGLETON);
        binder.bind(Executor.class).annotatedWith(ForDruidClient.class)
                .toInstance(newCachedThreadPool(threadsNamed("druid-metadata-fetcher-" + catalogName)));
    }
}
