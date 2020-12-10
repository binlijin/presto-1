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

import com.druid.hdfs.reader.utils.HDFSIOUtils;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.IOE;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

public class HDFSVersion
{
    private HDFSVersion()
    {
    }

    public static int getVersionFromDir(FileSystem fileSystem, Path inDir) throws IOException
    {
        Path versionFile = new Path(inDir, "version.bin");
        if (fileSystem.exists(versionFile)) {
            FSDataInputStream is = fileSystem.open(versionFile);
            byte[] content = new byte[4];
            IOUtils.readFully(is, content, 0, content.length);
            HDFSIOUtils.closeQuietly(is);
            return Ints.fromByteArray(content);
        }

        throw new IOE("Invalid segment dir [%s]. Can't find either of version.bin", inDir);
    }
}
