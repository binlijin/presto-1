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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    private final HdfsConfiguration hdfsConfiguration;

    @Inject
    public HdfsEnvironment(
            HdfsConfiguration hdfsConfiguration)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
    }

    public Configuration getConfiguration(HdfsContext context, Path path)
    {
        return hdfsConfiguration.getConfiguration(context, path.toUri());
    }

    public FileSystem getFileSystem(HdfsContext context, Path path)
            throws IOException
    {
        return getFileSystem(context.getIdentity().getUser(), path, getConfiguration(context, path));
    }

    public FileSystem getFileSystem(String user, Path path, Configuration configuration)
            throws IOException
    {
        // TODO
        return path.getFileSystem(configuration);
    }
}
