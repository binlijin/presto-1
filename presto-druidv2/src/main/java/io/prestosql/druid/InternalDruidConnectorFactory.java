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

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.prestosql.cache.CachingModule;
import io.prestosql.druid.authentication.DruidAuthenticationModule;
import io.prestosql.druid.ingestion.DruidPageSinkProvider;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.type.TypeManager;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class InternalDruidConnectorFactory
{
    private InternalDruidConnectorFactory() {}

    public static Connector createConnector(String catalogName, Map<String, String> config, ConnectorContext context, Module module)
    {
        requireNonNull(config, "config is null");

        ClassLoader classLoader = InternalDruidConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new DruidModule(),
                    new DruidAuthenticationModule(),
                    new CachingModule(),
                    binder -> {
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        //binder.bind(FunctionMetadataManager.class).toInstance(context.getFunctionMetadataManager());
                        //binder.bind(RowExpressionService.class).toInstance(context.getRowExpressionService());
                        //binder.bind(StandardFunctionResolution.class).toInstance(context.getStandardFunctionResolution());
                        //binder.bind(DeterminismEvaluator.class).toInstance(context.getRowExpressionService().getDeterminismEvaluator());
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            DruidMetadata metadata = injector.getInstance(DruidMetadata.class);
            DruidSplitManager splitManager = injector.getInstance(DruidSplitManager.class);
            DruidPageSourceProvider pageSourceProvider = injector.getInstance(DruidPageSourceProvider.class);
            DruidPageSinkProvider pageSinkProvider = injector.getInstance(DruidPageSinkProvider.class);
            DruidSessionProperties druidSessionProperties = injector.getInstance(DruidSessionProperties.class);
            return new DruidConnector(lifeCycleManager, metadata, splitManager, pageSourceProvider,
                    pageSinkProvider, druidSessionProperties);
            //return injector.getInstance(DruidConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
