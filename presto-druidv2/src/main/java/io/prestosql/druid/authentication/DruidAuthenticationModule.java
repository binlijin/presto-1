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
package io.prestosql.druid.authentication;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.druid.DruidConfig;
import io.prestosql.druid.ForDruidClient;

import java.util.function.Predicate;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.prestosql.druid.DruidConfig.DruidAuthenticationType.BASIC;
import static io.prestosql.druid.DruidConfig.DruidAuthenticationType.KERBEROS;
import static io.prestosql.druid.DruidConfig.DruidAuthenticationType.NONE;

public class DruidAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindAuthenticationModule(
                config -> config.getDruidAuthenticationType() == NONE,
                noneAuthenticationModule());

        bindAuthenticationModule(
                config -> config.getDruidAuthenticationType() == BASIC,
                basicAuthenticationModule());

        bindAuthenticationModule(
                config -> config.getDruidAuthenticationType() == KERBEROS,
                kerberosbAuthenticationModule());
    }

    private void bindAuthenticationModule(Predicate<DruidConfig> predicate, Module module)
    {
        install(installModuleIf(DruidConfig.class, predicate, module));
    }

    private static Module noneAuthenticationModule()
    {
        return binder -> httpClientBinder(binder).bindHttpClient("druid-client", ForDruidClient.class);
    }

    private static Module basicAuthenticationModule()
    {
        return binder -> httpClientBinder(binder).bindHttpClient("druid-client", ForDruidClient.class)
                .withConfigDefaults(
                        config -> config.setAuthenticationEnabled(false) //disable Kerberos auth
                ).withFilter(
                        DruidBasicAuthHttpRequestFilter.class);
    }

    private static Module kerberosbAuthenticationModule()
    {
        return binder -> httpClientBinder(binder).bindHttpClient("druid-client", ForDruidClient.class)
                .withConfigDefaults(
                        config -> config.setAuthenticationEnabled(true));
    }
}