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
package io.prestosql.cache.alluxio;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class AlluxioCacheConfig
{
    private boolean metricsCollectionEnabled = true;
    private boolean asyncWriteEnabled;
    private String jmxClass = "alluxio.metrics.sink.JmxSink";
    private String metricsDomain = "com.facebook.alluxio";
    private DataSize maxCacheSize = DataSize.of(2, GIGABYTE);
    private DataSize pageCacheSize = DataSize.of(1, MEGABYTE);
    private boolean configValidationEnabled;

    public boolean isMetricsCollectionEnabled()
    {
        return metricsCollectionEnabled;
    }

    @Config("cache.alluxio.metrics-enabled")
    @ConfigDescription("If metrics for alluxio caching are enabled")
    public AlluxioCacheConfig setMetricsCollectionEnabled(boolean metricsCollectionEnabled)
    {
        this.metricsCollectionEnabled = metricsCollectionEnabled;
        return this;
    }

    public String getJmxClass()
    {
        return jmxClass;
    }

    @Config("cache.alluxio.jmx-class")
    @ConfigDescription("JMX class name used by the alluxio caching for metrics")
    public AlluxioCacheConfig setJmxClass(String jmxClass)
    {
        this.jmxClass = jmxClass;
        return this;
    }

    public String getMetricsDomain()
    {
        return metricsDomain;
    }

    @Config("cache.alluxio.metrics-domain")
    @ConfigDescription("Metrics domain name used by the alluxio caching")
    public AlluxioCacheConfig setMetricsDomain(String metricsDomain)
    {
        this.metricsDomain = metricsDomain;
        return this;
    }

    public boolean isAsyncWriteEnabled()
    {
        return asyncWriteEnabled;
    }

    @Config("cache.alluxio.async-write-enabled")
    @ConfigDescription("If alluxio caching should write to cache asynchronously")
    public AlluxioCacheConfig setAsyncWriteEnabled(boolean asyncWriteEnabled)
    {
        this.asyncWriteEnabled = asyncWriteEnabled;
        return this;
    }

    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("cache.alluxio.max-cache-size")
    @ConfigDescription("The maximum cache size available for alluxio cache")
    public AlluxioCacheConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    public DataSize getPageCacheSize()
    {
        return pageCacheSize;
    }

    @Config("cache.alluxio.page-cache-size")
    @ConfigDescription("Size of each page in client-side cache for alluxio cache")
    public AlluxioCacheConfig setPageCacheSize(DataSize pageCacheSize)
    {
        this.pageCacheSize = pageCacheSize;
        return this;
    }

    @Config("cache.alluxio.config-validation-enabled")
    @ConfigDescription("If the alluxio caching should validate the provided configuration")
    public AlluxioCacheConfig setConfigValidationEnabled(boolean configValidationEnabled)
    {
        this.configValidationEnabled = configValidationEnabled;
        return this;
    }

    public boolean isConfigValidationEnabled()
    {
        return configValidationEnabled;
    }
}