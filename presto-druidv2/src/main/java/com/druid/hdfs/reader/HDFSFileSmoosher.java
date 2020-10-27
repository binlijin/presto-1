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
package com.druid.hdfs.reader;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

public class HDFSFileSmoosher
        implements Closeable
{
    private static final String FILE_EXTENSION = "smoosh";

    static Path metaFile(Path baseDir)
    {
        return new Path(baseDir, StringUtils.format("meta.%s", FILE_EXTENSION));
    }

    static String makeChunkFile(int i)
    {
        return StringUtils.format("%05d.%s", i, FILE_EXTENSION);
    }

    @Override public void close() throws IOException
    {
    }
}
