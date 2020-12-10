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
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.druid.java.util.common.ISE;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class HDFSSmooshedFileMapper
        implements Closeable
{
    public static HDFSSmooshedFileMapper load(FileSystem fs, Path baseDir) throws IOException
    {
        Path metaFile = HDFSFileSmoosher.metaFile(baseDir);

        BufferedReader in = null;
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = fs.open(metaFile);
            in = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8));

            String line = in.readLine();
            if (line == null) {
                throw new ISE("First line should be version,maxChunkSize,numChunks, got null.");
            }

            String[] splits = line.split(",");
            if (!"v1".equals(splits[0])) {
                throw new ISE("Unknown version[%s], v1 is all I know.", splits[0]);
            }
            if (splits.length != 3) {
                throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
            }
            final Integer numFiles = Integer.valueOf(splits[2]);
            List<String> outFiles = Lists.newArrayListWithExpectedSize(numFiles);

            for (int i = 0; i < numFiles; ++i) {
                outFiles.add(HDFSFileSmoosher.makeChunkFile(i));
            }

            Map<String, HDFSMetadata> internalFiles = new TreeMap<>();
            while ((line = in.readLine()) != null) {
                splits = line.split(",");

                if (splits.length != 4) {
                    throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
                }
                internalFiles.put(splits[0],
                        new HDFSMetadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]),
                                Integer.parseInt(splits[3])));
            }

            return new HDFSSmooshedFileMapper(outFiles, internalFiles);
        }
        finally {
            HDFSIOUtils.closeQuietly(fsDataInputStream);
            Closeables.close(in, false);
        }
    }

    private final List<String> outFiles;
    private final Map<String, HDFSMetadata> internalFiles;

    private HDFSSmooshedFileMapper(List<String> outFiles, Map<String, HDFSMetadata> internalFiles)
    {
        this.outFiles = outFiles;
        this.internalFiles = internalFiles;
    }

    public Set<String> getInternalFilenames()
    {
        return internalFiles.keySet();
    }

    public HDFSMetadata mapFile(String name)
    {
        return internalFiles.get(name);
    }

    public String getFileNum(int fileNum)
    {
        return outFiles.get(fileNum);
    }

    @Override
    public void close()
    {
    }
}
