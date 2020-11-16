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

import com.druid.hdfs.reader.column.HDFSColumnDescriptor;
import com.druid.hdfs.reader.column.HDFSStringDictionaryEncodedColumn;
import com.druid.hdfs.reader.utils.HDFSByteBuff;
import com.druid.hdfs.reader.utils.HDFSSerializerUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexIndexableAdapter;
import org.apache.druid.segment.RowIterator;
import org.apache.druid.segment.RowPointer;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.DoublesColumn;
import org.apache.druid.segment.column.LongsColumn;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.druid.segment.data.GenericIndexed.STRING_STRATEGY;

public class HDFSIndexIO
{
    static {
        NullHandling.initializeForTests();
    }

    private static final String FILE_EXTENSION = "smoosh";
    public static final byte V8_VERSION = 0x8;
    public static final byte V9_VERSION = 0x9;
    public static final int CURRENT_VERSION_ID = V9_VERSION;
    public static final BitmapSerdeFactory LEGACY_FACTORY =
            new BitmapSerde.LegacyBitmapSerdeFactory();

    private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

    private static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

    private static final com.druid.hdfs.reader.utils.HDFSSerializerUtils HDFSSERIALIZER_UTILS =
            new HDFSSerializerUtils();

    public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

    private static final EmittingLogger log = new EmittingLogger(IndexIO.class);

    private final Map<Integer, HDFSIndexLoader> indexLoaders;

    private final ObjectMapper mapper;

    public HDFSIndexIO(ObjectMapper mapper, ColumnConfig columnConfig)
    {
        this.mapper = requireNonNull(mapper, "null ObjectMapper");

        ImmutableMap.Builder<Integer, HDFSIndexLoader> indexLoadersBuilder = ImmutableMap.builder();
        indexLoadersBuilder.put((int) V9_VERSION, new HDFSV9IndexLoader(columnConfig));
        indexLoaders = indexLoadersBuilder.build();
    }

    public QueryableIndex loadIndex(Path path) throws IOException
    {
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        if (fileSystem.exists(path)) {
            final int version = HDFSVersion.getVersionFromDir(fileSystem, path);
            final HDFSIndexLoader loader = indexLoaders.get(version);

            if (loader != null) {
                return loader.load(fileSystem, path, mapper, false);
            }
            else {
                throw new ISE("Unknown index version[%s]", version);
            }
        }
        return null;
    }

    public QueryableIndex loadIndex(FileSystem fileSystem, Path path, List<String> columns)
            throws IOException
    {
        if (fileSystem.exists(path)) {
            final int version = HDFSVersion.getVersionFromDir(fileSystem, path);
            final HDFSIndexLoader loader = indexLoaders.get(version);

            if (loader != null) {
                return loader.load(fileSystem, path, columns, mapper, false);
            }
            else {
                throw new ISE("Unknown index version[%s]", version);
            }
        }
        return null;
    }

    interface HDFSIndexLoader
    {
        QueryableIndex load(FileSystem fileSystem, Path inDir, ObjectMapper mapper, boolean lazy)
                throws IOException;

        QueryableIndex load(FileSystem fileSystem, Path inDir, List<String> columns,
                ObjectMapper mapper, boolean lazy)
                throws IOException;
    }

    static class HDFSV9IndexLoader
            implements HDFSIndexLoader
    {
        private final ColumnConfig columnConfig;

        HDFSV9IndexLoader(ColumnConfig columnConfig)
        {
            this.columnConfig = columnConfig;
        }

        @Override
        public QueryableIndex load(FileSystem fileSystem, Path inDir, ObjectMapper mapper,
                boolean lazy) throws IOException
        {
            return load(fileSystem, inDir, null, mapper, lazy);
        }

        @Override
        public QueryableIndex load(FileSystem fileSystem, Path inDir, List<String> selColumns,
                ObjectMapper mapper, boolean lazy) throws IOException
        {
            log.debug("Mapping v9 index[%s]", inDir);
            long startTime = System.currentTimeMillis();
            final int theVersion = HDFSVersion.getVersionFromDir(fileSystem, inDir);
            if (theVersion != V9_VERSION) {
                throw new IAE("Expected version[9], got[%d]", theVersion);
            }

            HDFSSmooshedFileMapper smooshedFiles = HDFSSmooshedFileMapper.load(fileSystem, inDir);

            HDFSMetadata indexMetadata = smooshedFiles.mapFile("index.drd");
            //00000.smoosh
            String fileName = smooshedFiles.getFileNum(indexMetadata.getFileNum());
            Path smooshFile = new Path(inDir, fileName);
            FSDataInputStream is = fileSystem.open(smooshFile);
            int fileSize = indexMetadata.getEndOffset() - indexMetadata.getStartOffset();
            byte[] buffer = new byte[fileSize];
            is.readFully(indexMetadata.getStartOffset(), buffer);
            ByteBuffer indexBuffer = ByteBuffer.wrap(buffer);

            //      HDFSByteBuff byteBuff = new HDFSByteBuff(is, indexMetadata.getStartOffset(), fileSize);
            //      HDFSGenericIndexed<String> cols1 = HDFSGenericIndexed.read(byteBuff, STRING_STRATEGY);
            //      HDFSGenericIndexed<String> dims1 = HDFSGenericIndexed.read(byteBuff, STRING_STRATEGY);

            /**
             * Index.drd should consist of the segment version, the columns and dimensions of the segment as generic
             * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
             */
            GenericIndexed<String> cols = GenericIndexed.read(indexBuffer, STRING_STRATEGY);
            GenericIndexed<String> dims = GenericIndexed.read(indexBuffer, STRING_STRATEGY);

            //      final Interval dataInterval1 = Intervals.utc(byteBuff.getLong(), byteBuff.getLong());

            //      final BitmapSerdeFactory segmentBitmapSerdeFactory1;
            //      if (indexBuffer.hasRemaining()) {
            //        segmentBitmapSerdeFactory1 =
            //            mapper.readValue(HDFSSERIALIZER_UTILS.readString(byteBuff), BitmapSerdeFactory.class);
            //      } else {
            //        segmentBitmapSerdeFactory1 = new BitmapSerde.LegacyBitmapSerdeFactory();
            //      }

            final Interval dataInterval =
                    Intervals.utc(indexBuffer.getLong(), indexBuffer.getLong());

            final BitmapSerdeFactory segmentBitmapSerdeFactory;
            /**
             * This is a workaround for the fact that in v8 segments, we have no information about the type of bitmap
             * index to use. Since we cannot very cleanly build v9 segments directly, we are using a workaround where
             * this information is appended to the end of index.drd.
             */
            if (indexBuffer.hasRemaining()) {
                segmentBitmapSerdeFactory =
                        mapper.readValue(SERIALIZER_UTILS.readString(indexBuffer),
                                BitmapSerdeFactory.class);
            }
            else {
                segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
            }
            //log.debug("segmentBitmapSerdeFactory  = %s", segmentBitmapSerdeFactory);

            HDFSMetadata metadataDrd = smooshedFiles.mapFile("metadata.drd");
            String metaDataDrdfileName = smooshedFiles.getFileNum(metadataDrd.getFileNum());
            if (!fileName.equals(metaDataDrdfileName)) {
                smooshFile = new Path(inDir, metaDataDrdfileName);
                is = fileSystem.open(smooshFile);
            }
            byte[] metadatabuffer =
                    new byte[metadataDrd.getEndOffset() - metadataDrd.getStartOffset()];
            is.readFully(metadataDrd.getStartOffset(), metadatabuffer);
            ByteBuffer metadataBB = ByteBuffer.wrap(metadatabuffer);

            org.apache.druid.segment.Metadata metadata = null;
            if (metadataBB != null) {
                try {
                    metadata = mapper.readValue(
                            SERIALIZER_UTILS.readBytes(metadataBB, metadataBB.remaining()),
                            org.apache.druid.segment.Metadata.class);
                }
                catch (JsonParseException | JsonMappingException ex) {
                    // Any jackson deserialization errors are ignored e.g. if metadata contains some aggregator which
                    // is no longer supported then it is OK to not use the metadata instead of failing segment loading
                    // log.warn(ex, "Failed to load metadata for segment [%s]", inDir);
                    // No 'injectableValues' configured, cannot inject value with id [org.apache.druid.math.expr.ExprMacroTable]
                }
                catch (IOException ex) {
                    throw new IOException("Failed to read metadata", ex);
                }
            }
            //log.debug("metadata = %s", metadata);

            Map<String, Supplier<ColumnHolder>> columns = new HashMap<>();

            List<String> selectCs = null;
            if (selColumns == null || selColumns.isEmpty()) {
                selectCs = Lists.newArrayList(cols);
            }
            else {
                selectCs = selColumns;
            }
            for (String columnName : selectCs) {
                if (Strings.isNullOrEmpty(columnName)) {
                    log.warn("Null or Empty Dimension found in the file : " + inDir);
                    continue;
                }

                HDFSMetadata columnMetaDataDrd = smooshedFiles.mapFile(columnName);
                String columnMetaDataDrdfileName =
                        smooshedFiles.getFileNum(columnMetaDataDrd.getFileNum());
                if (!fileName.equals(columnMetaDataDrdfileName)) {
                    smooshFile = new Path(inDir, columnMetaDataDrdfileName);
                    is = fileSystem.open(smooshFile);
                }

                int columnSize =
                        columnMetaDataDrd.getEndOffset() - columnMetaDataDrd.getStartOffset();
                HDFSByteBuff columnByteBuff =
                        new HDFSByteBuff(is, columnMetaDataDrd.getStartOffset(), columnSize);

                //        byte[] columnDataBuffer = new byte[columnSize];
                //        is.readFully(columnMetaDataDrd.getStartOffset(), columnDataBuffer);
                //        ByteBuffer columnData = ByteBuffer.wrap(columnDataBuffer);

                if (lazy) {
                    columns.put(columnName, Suppliers.memoize(() -> {
                        try {
                            //return deserializeColumn(mapper, columnData, null);
                            return deserializeColumn(mapper, columnByteBuff, null);
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }));
                }
                else {
                    //ColumnHolder columnHolder = deserializeColumn(mapper, columnData, null);
                    ColumnHolder columnHolder = deserializeColumn(mapper, columnByteBuff, null);
                    columns.put(columnName, () -> columnHolder);
                }
            }

            // time
            //printLongColumn(ColumnHolder.TIME_COLUMN_NAME, smooshedFiles, fileSystem, inDir);
            //printLongColumn("count", smooshedFiles, fileSystem, inDir);

            // added  deleted  delta
            //printDoubleColumn("added", smooshedFiles, fileSystem, inDir);

            //String columnName = "namespace"; // page user  isNew isRobot namespace channel
            //printStringColumn(columnName, smooshedFiles, fileSystem, inDir);

            HDFSMetadata timeMetadata = smooshedFiles.mapFile(ColumnHolder.TIME_COLUMN_NAME);
            String timeMetaDataDrdfileName = smooshedFiles.getFileNum(timeMetadata.getFileNum());
            if (timeMetaDataDrdfileName.equals(fileName)) {
                smooshFile = new Path(inDir, timeMetaDataDrdfileName);
                is = fileSystem.open(smooshFile);
            }
            HDFSByteBuff timeByteBuff = new HDFSByteBuff(is, timeMetadata.getStartOffset(),
                    timeMetadata.getEndOffset() - timeMetadata.getStartOffset());

            //      byte[] columnDataBuffer = new byte[timeMetadata.getEndOffset() - timeMetadata.getStartOffset()];
            //      is.readFully(timeMetadata.getStartOffset(), columnDataBuffer);
            //      ByteBuffer columnData = ByteBuffer.wrap(columnDataBuffer);

            if (lazy) {
                columns.put(ColumnHolder.TIME_COLUMN_NAME, Suppliers.memoize(() -> {
                    try {
                        //return deserializeColumn(mapper, columnData, null);
                        return deserializeColumn(mapper, timeByteBuff, null);
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }));
            }
            else {
                //ColumnHolder columnHolder = deserializeColumn(mapper, columnData, null);
                ColumnHolder columnHolder = deserializeColumn(mapper, timeByteBuff, null);
                columns.put(ColumnHolder.TIME_COLUMN_NAME, () -> columnHolder);
            }

            Indexed<String> indexed = new ListIndexed<>(selectCs);
            final QueryableIndex index = new HDFSSimpleQueryableIndex(
                    dataInterval,
                    indexed,
                    segmentBitmapSerdeFactory.getBitmapFactory(),
                    columns,
                    smooshedFiles,
                    metadata,
                    lazy);

            log.debug("Mapped v9 index[%s] in %,d millis", inDir,
                    System.currentTimeMillis() - startTime);

            return index;
        }

        private void printLongColumn(String columnName, HDFSSmooshedFileMapper smooshedFiles,
                FileSystem fileSystem, Path inDir) throws IOException
        {
            System.out.println("");
            System.out.println("printLongColumn " + columnName);
            HDFSMetadata columnMetaDataDrd = smooshedFiles.mapFile(columnName);
            String columnMetaDataDrdfileName =
                    smooshedFiles.getFileNum(columnMetaDataDrd.getFileNum());
            Path smooshFile = new Path(inDir, columnMetaDataDrdfileName);
            FSDataInputStream is = fileSystem.open(smooshFile);
            byte[] columnDataBuffer =
                    new byte[columnMetaDataDrd.getEndOffset() - columnMetaDataDrd.getStartOffset()];
            is.readFully(columnMetaDataDrd.getStartOffset(), columnDataBuffer);
            ColumnHolder columnHolder = createColumnHolder(columnDataBuffer);
            if (log.isDebugEnabled()) {
                log.debug(" " + columnHolder.toString());
                log.debug(" " + columnHolder.getLength());
                log.debug(" " + columnHolder.getColumn());
                int length = columnHolder.getLength();
                LongsColumn longsColumn = (LongsColumn) columnHolder.getColumn();
                for (int i = 0; i < length; i++) {
                    //log.debug(" " + longsColumn.getLongSingleValueRow(i));
                }
            }

            HDFSByteBuff columnByteBuff = new HDFSByteBuff(is, columnMetaDataDrd.getStartOffset(),
                    columnMetaDataDrd.getEndOffset() - columnMetaDataDrd.getStartOffset());
            ColumnHolder columnHolder2 = createColumnHolder(columnByteBuff);
            if (log.isDebugEnabled()) {
                log.debug(" " + columnHolder2.toString());
                log.debug(" " + columnHolder2.getLength());
                log.debug(" " + columnHolder2.getColumn());
                int length = columnHolder2.getLength();
                LongsColumn longsColumn = (LongsColumn) columnHolder2.getColumn();
                for (int i = 0; i < length; i++) {
                    //log.debug(" " + longsColumn.getLongSingleValueRow(i));
                }
                compare(longsColumn, (LongsColumn) columnHolder.getColumn());
            }
        }

        private void printDoubleColumn(String columnName, HDFSSmooshedFileMapper smooshedFiles,
                FileSystem fileSystem, Path inDir) throws IOException
        {
            System.out.println("");
            System.out.println("printDoubleColumn " + columnName);
            HDFSMetadata columnMetaDataDrd = smooshedFiles.mapFile(columnName);
            String columnMetaDataDrdfileName =
                    smooshedFiles.getFileNum(columnMetaDataDrd.getFileNum());
            Path smooshFile = new Path(inDir, columnMetaDataDrdfileName);
            FSDataInputStream is = fileSystem.open(smooshFile);
            byte[] columnDataBuffer =
                    new byte[columnMetaDataDrd.getEndOffset() - columnMetaDataDrd.getStartOffset()];
            is.readFully(columnMetaDataDrd.getStartOffset(), columnDataBuffer);
            ColumnHolder columnHolder = createColumnHolder(columnDataBuffer);
            if (log.isDebugEnabled()) {
                log.debug(" " + columnHolder.toString());
                log.debug(" " + columnHolder.getLength());
                log.debug(" " + columnHolder.getColumn());
                int length = columnHolder.getLength();
                DoublesColumn doublesColumn = (DoublesColumn) columnHolder.getColumn();
                final SimpleAscendingOffset offset =
                        new SimpleAscendingOffset(columnHolder.getLength());
                ColumnValueSelector<?> selector = doublesColumn.makeColumnValueSelector(offset);
                while (offset.withinBounds()) {
                    //log.debug("" + selector.getDouble());
                    offset.increment();
                }
                for (int i = 0; i < length; i++) {
                    //log.debug(" " + doublesColumn.getLongSingleValueRow(i));
                }
            }

            HDFSByteBuff columnByteBuff = new HDFSByteBuff(is, columnMetaDataDrd.getStartOffset(),
                    columnMetaDataDrd.getEndOffset() - columnMetaDataDrd.getStartOffset());
            ColumnHolder columnHolder2 = createColumnHolder(columnByteBuff);
            if (log.isDebugEnabled()) {
                log.debug(" " + columnHolder2.toString());
                log.debug(" " + columnHolder2.getLength());
                log.debug(" " + columnHolder2.getColumn());
                int length = columnHolder2.getLength();
                DoublesColumn doublesColumn = (DoublesColumn) columnHolder2.getColumn();
                final SimpleAscendingOffset offset =
                        new SimpleAscendingOffset(columnHolder.getLength());
                ColumnValueSelector<?> selector = doublesColumn.makeColumnValueSelector(offset);
                while (offset.withinBounds()) {
                    //log.debug("" + selector.getDouble());
                    offset.increment();
                }
                for (int i = 0; i < length; i++) {
                    //log.debug(" " + doublesColumn.getLongSingleValueRow(i));
                }
                compare(doublesColumn, (DoublesColumn) columnHolder.getColumn());
            }
        }

        private void printStringColumn(String columnName, HDFSSmooshedFileMapper smooshedFiles,
                FileSystem fileSystem, Path inDir) throws IOException
        {
            System.out.println("");
            System.out.println("printStringColumn " + columnName);
            HDFSMetadata columnMetaDataDrd = smooshedFiles.mapFile(columnName);
            String columnMetaDataDrdfileName =
                    smooshedFiles.getFileNum(columnMetaDataDrd.getFileNum());
            Path smooshFile = new Path(inDir, columnMetaDataDrdfileName);
            FSDataInputStream is = fileSystem.open(smooshFile);

            byte[] columnDataBuffer =
                    new byte[columnMetaDataDrd.getEndOffset() - columnMetaDataDrd.getStartOffset()];
            is.readFully(columnMetaDataDrd.getStartOffset(), columnDataBuffer);
            ColumnHolder columnHolder = createColumnHolder(columnDataBuffer);
            log.info("StringColumn    " + columnName + " " + columnHolder.getColumn().toString());
            if (log.isDebugEnabled()) {
                log.debug(" " + columnHolder.toString());
                StringDictionaryEncodedColumn strCol =
                        (StringDictionaryEncodedColumn) columnHolder.getColumn();

                log.debug("length = " + strCol.length() + ", ValueCardinality = " + strCol
                        .getCardinality());
                for (int i = 0; i < strCol.length(); i++) {
                    int v = strCol.getSingleValueRow(i);
                    //log.debug(" " + v + ", " + strCol.lookupName(v));
                }
                for (int i = 0; i < strCol.getCardinality(); i++) {
                    //log.debug(i + " " + strCol.lookupName(i));
                }
            }

            HDFSByteBuff columnByteBuff = new HDFSByteBuff(is, columnMetaDataDrd.getStartOffset(),
                    columnMetaDataDrd.getEndOffset() - columnMetaDataDrd.getStartOffset());
            ColumnHolder columnHolder2 = createColumnHolder(columnByteBuff);
            if (log.isDebugEnabled()) {
                log.debug(" " + columnHolder2.toString());
                HDFSStringDictionaryEncodedColumn strCol =
                        (HDFSStringDictionaryEncodedColumn) columnHolder2.getColumn();
                BitmapIndex bitmapIndex = columnHolder2.getBitmapIndex();
                log.debug("length = " + strCol.length() + ", ValueCardinality = " + strCol
                        .getCardinality());
                for (int i = 0; i < strCol.length(); i++) {
                    int v = strCol.getSingleValueRow(i);
                    //log.debug(i + ", " + v + ", " + strCol.lookupName(v));
                }
                for (int i = 0; i < strCol.getCardinality(); i++) {
                    String dic = strCol.lookupName(i);
                    int index = bitmapIndex.getIndex(dic);
                    ImmutableBitmap bitmap = bitmapIndex.getBitmap(index);
                    Set<Integer> s3 = new HashSet<>();
                    IntIterator iter = bitmap.iterator();
                    //log.debug(i + ", " + dic);
                    while (iter.hasNext()) {
                        s3.add(iter.next());
                    }
                    //log.debug("         bitmap =   " + s3);
                }
            }
        }

        private void compare(LongsColumn left, LongsColumn right)
        {
            boolean equals = true;
            for (int i = 0; i < left.length(); i++) {
                if (left.getLongSingleValueRow(i) != right.getLongSingleValueRow(i)) {
                    equals = false;
                    break;
                }
            }
            if (left.length() != right.length()) {
                equals = false;
            }
            System.out.println(equals);
        }

        private void compare(DoublesColumn left, DoublesColumn right)
        {
            boolean equals = true;
            for (int i = 0; i < left.length(); i++) {
                if (left.getLongSingleValueRow(i) != right.getLongSingleValueRow(i)) {
                    equals = false;
                    break;
                }
            }
            if (left.length() != right.length()) {
                equals = false;
            }
            System.out.println(equals);
        }

        private ColumnDescriptor readColumnDescriptor(HDFSByteBuff byteBuff) throws IOException
        {
            return JSON_MAPPER
                    .readValue(HDFSSERIALIZER_UTILS.readString(byteBuff), ColumnDescriptor.class);
        }

        private ColumnDescriptor readColumnDescriptor(ByteBuffer byteBuffer) throws IOException
        {
            return JSON_MAPPER
                    .readValue(SERIALIZER_UTILS.readString(byteBuffer), ColumnDescriptor.class);
        }

        private ColumnHolder createColumnHolder(byte[] columnDataBytes) throws IOException
        {
            ByteBuffer columnData = ByteBuffer.wrap(columnDataBytes);
            ColumnDescriptor columnDescriptor = readColumnDescriptor(columnData);
            return columnDescriptor.read(columnData, () -> 0, null);
        }

        private ColumnHolder createColumnHolder(HDFSByteBuff byteBuff) throws IOException
        {
            ColumnDescriptor columnDescriptor = readColumnDescriptor(byteBuff);
            HDFSColumnDescriptor hdfsColumnDescriptor = new HDFSColumnDescriptor(columnDescriptor);
            return hdfsColumnDescriptor.read(byteBuff, () -> 0, null);
        }

        private ColumnHolder deserializeColumn(ObjectMapper mapper, HDFSByteBuff byteBuffer,
                SmooshedFileMapper smooshedFiles) throws IOException
        {
            ColumnDescriptor serde = mapper.readValue(HDFSSERIALIZER_UTILS.readString(byteBuffer),
                    ColumnDescriptor.class);
            HDFSColumnDescriptor hdfsColumnDescriptor = new HDFSColumnDescriptor(serde);
            return hdfsColumnDescriptor.read(byteBuffer, columnConfig, smooshedFiles);
        }

        private ColumnHolder deserializeColumn(ObjectMapper mapper, ByteBuffer byteBuffer,
                SmooshedFileMapper smooshedFiles) throws IOException
        {
            ColumnDescriptor serde = mapper.readValue(SERIALIZER_UTILS.readString(byteBuffer),
                    ColumnDescriptor.class);
            return serde.read(byteBuffer, columnConfig, smooshedFiles);
        }
    }

    private static String makeChunkFileName(int i)
    {
        return format("%05d.%s", i, FILE_EXTENSION);
    }

    public static void main(String[] args)
    {
        System.out.println(CompressedPools.BUFFER_SIZE);
        System.out.println(CompressedPools.BUFFER_SIZE / Long.BYTES);

        HDFSIndexIO hdfsIndexIO = new HDFSIndexIO(new DefaultObjectMapper(), () -> 0);
        //    Path path = new Path(
        //        "hdfs://localhost:9000/user/druid/wikipedia/20150912T000000.000Z_20150913T000000.000Z/2020-08-11T12_07_36.414Z/0/");

        // hdfs://localhost:9000/user/druid/wikipedia3/20150912T000000.000Z_20150913T000000.000Z/2020-09-22T09_26_46.650Z/0/
        // hdfs://localhost:9000/tmp/data/index/output/wikipedia3/20150912T000000.000Z_20150913T000000.000Z/2020-10-26T12_21_22.234Z/0
        Path path = new Path(
                "hdfs://localhost:9000/tmp/data/index/output/wikipedia3/20150912T000000.000Z_20150913T000000.000Z/2020-10-26T12_21_22.234Z/0");

        try {
            long startTime = System.currentTimeMillis();
            QueryableIndex queryableIndex = hdfsIndexIO.loadIndex(path);
            IndexableAdapter adapter = new QueryableIndexIndexableAdapter(queryableIndex);
            RowIterator it = adapter.getRows();
            int num = 0;
            while (it.moveToNext()) {
                RowPointer row = it.getPointer();
                row.toString();
                //System.out.println(row);
                num++;
            }
            System.out.println(num + " rows.");

            System.out.println(System.currentTimeMillis() - startTime + " millis.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
