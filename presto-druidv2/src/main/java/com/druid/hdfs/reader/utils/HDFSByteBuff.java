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
package com.druid.hdfs.reader.utils;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

public class HDFSByteBuff
        extends ByteBuff
{
    private FSDataInputStream is;
    private final int offset;

    private int mark = -1;
    private int position;
    private int limit;
    private int capacity;

    public HDFSByteBuff(FSDataInputStream is, int offset, int length)
    {
        this.is = is;
        this.mark = -1;
        this.position = 0;
        this.limit = length;
        this.capacity = length;
        this.offset = offset;
    }

    public HDFSByteBuff(FSDataInputStream is, int mark, int pos, int lim, int cap, int off)
    {
        this.is = is;
        this.mark = mark;
        this.position = pos;
        this.limit = lim;
        this.capacity = cap;
        this.offset = off;
    }

    @Override public int position()
    {
        return position;
    }

    @Override public HDFSByteBuff position(int newPosition)
    {
        if ((newPosition > limit) || (newPosition < 0)) {
            throw new IllegalArgumentException();
        }
        position = newPosition;
        if (mark > position) {
            mark = -1;
        }
        return this;
    }

    @Override public HDFSByteBuff skip(int len)
    {
        position = position + len;
        return this;
    }

    @Override public HDFSByteBuff moveBack(int len)
    {
        position = position - len;
        return this;
    }

    @Override public int capacity()
    {
        return capacity;
    }

    @Override public int limit()
    {
        return limit;
    }

    @Override public HDFSByteBuff limit(int newLimit)
    {
        if ((newLimit > capacity) || (newLimit < 0)) {
            throw new IllegalArgumentException();
        }
        limit = newLimit;
        if (position > limit) {
            position = limit;
        }
        if (mark > limit) {
            mark = -1;
        }
        return this;
    }

    @Override public HDFSByteBuff rewind()
    {
        position = 0;
        mark = -1;
        return this;
    }

    @Override public HDFSByteBuff mark()
    {
        mark = position;
        return this;
    }

    @Override public int remaining()
    {
        return limit - position;
    }

    @Override public boolean hasRemaining()
    {
        return position < limit;
    }

    @Override public HDFSByteBuff reset()
    {
        int m = mark;
        if (m < 0) {
            throw new InvalidMarkException();
        }
        position = m;
        return this;
    }

    @Override public HDFSByteBuff slice()
    {
        return new HDFSByteBuff(is, -1, 0, this.remaining(), this.remaining(),
                this.position() + offset);
    }

    @Override public HDFSByteBuff duplicate()
    {
        return new HDFSByteBuff(is, mark, this.position(), this.limit(), this.capacity(), offset);
    }

    @Override public byte get() throws IOException
    {
        return readByte(ix(nextGetIndex()));
    }

    @Override public byte get(int index) throws IOException
    {
        return readByte(ix(checkIndex(index)));
    }

    @Override public byte getByteAfterPosition(int offset)
    {
        //return get(this.buf.position() + offset);
        // TODO
        return 0;
    }

    @Override public HDFSByteBuff put(byte b)
    {
        //this.buf.put(b);
        // TODO
        return this;
    }

    @Override public HDFSByteBuff put(int index, byte b)
    {
        //buf.put(index, b);
        // TODO
        return this;
    }

    @Override public void get(byte[] dst, int offset, int length) throws IOException
    {
        checkBounds(offset, length, dst.length);
        if (length > remaining()) {
            throw new BufferUnderflowException();
        }
        is.readFully(ix(position()), dst, offset, length);
        position(position() + length);
    }

    @Override public void get(int sourceOffset, byte[] dst, int offset, int length)
            throws IOException
    {
        //    ByteBuffer inDup = this.buf.duplicate();
        //    inDup.position(sourceOffset);
        //    inDup.get(dst, offset, length);
        // TODO
        is.readFully(ix(sourceOffset), dst, offset, length);
    }

    @Override public void get(byte[] dst) throws IOException
    {
        get(dst, 0, dst.length);
    }

    @Override public HDFSByteBuff put(int offset, ByteBuff src, int srcOffset, int length)
    {
        //    ByteBuffer outDup = ((HDFSByteBuff) src).buf.duplicate();
        //    outDup.position(offset);
        //    ByteBuffer inDup = this.buf.duplicate();
        //    inDup.position(srcOffset).limit(srcOffset + length);
        //    outDup.put(inDup);
        // TODO
        return this;
    }

    @Override public HDFSByteBuff put(byte[] src, int offset, int length)
    {
        //this.buf.put(src, offset, length);
        // TODO
        return this;
    }

    @Override public HDFSByteBuff put(byte[] src)
    {
        return put(src, 0, src.length);
    }

    @Override public boolean hasArray()
    {
        return false;
    }

    @Override public byte[] array()
    {
        //return this.buf.array();
        // TODO
        return null;
    }

    @Override public int arrayOffset()
    {
        //return this.buf.arrayOffset();
        // TODO
        return 0;
    }

    @Override public short getShort()
    {
        //return this.buf.getShort();
        // TODO
        return 0;
    }

    @Override public short getShort(int index)
    {
        //return this.buf.getShort(index);
        // TODO
        return 0;
    }

    @Override public short getShortAfterPosition(int offset)
    {
        //return getShort(this.buf.position() + offset);
        // TODO
        return 0;
    }

    @Override public int getInt() throws IOException
    {
        return readInt(ix(nextGetIndex(4)));
    }

    @Override public HDFSByteBuff putInt(int value)
    {
        //this.buf.putInt(value);
        // TODO
        return this;
    }

    @Override public int getInt(int index) throws IOException
    {
        return readInt(ix(checkIndex(index, 4)));
    }

    @Override public int getIntAfterPosition(int offset)
    {
        //return getInt(this.buf.position() + offset);
        // TODO
        return 0;
    }

    @Override public long getLong() throws IOException
    {
        //return this.buf.getLong();
        return readLong(ix(nextGetIndex(8)));
    }

    @Override public HDFSByteBuff putLong(long value)
    {
        // TODO
        return this;
    }

    @Override public long getLong(int index)
    {
        //return this.buf.getLong(index);
        // TODO
        return 0;
    }

    @Override public long getLongAfterPosition(int offset)
    {
        //return getLong(this.buf.position() + offset);
        // TODO
        return 0;
    }

    @Override public byte[] toBytes(int offset, int length)
    {
        //    byte[] output = new byte[length];
        //    ByteBuffer inDup = buf.duplicate();
        //    inDup.position(offset);
        //    inDup.get(output, 0, length);
        //    return output;
        // TODO
        return null;
    }

    @Override public void get(ByteBuffer out, int sourceOffset, int length)
    {
        //    ByteBuffer outDup = out.duplicate();
        //    outDup.position(0);
        //    ByteBuffer inDup = buf.duplicate();
        //    inDup.position(sourceOffset).limit(sourceOffset + length);
        //    outDup.put(inDup);
    }

    public final HDFSByteBuff clear()
    {
        position = 0;
        limit = capacity;
        mark = -1;
        return this;
    }

    @Override public boolean equals(Object obj)
    {
        if (!(obj instanceof HDFSByteBuff)) {
            return false;
        }
        return this.is.equals(((HDFSByteBuff) obj).is);
    }

    @Override public int hashCode()
    {
        return this.is.hashCode();
    }

    final int nextGetIndex()
    {
        if (position >= limit) {
            throw new BufferUnderflowException();
        }
        return position++;
    }

    final int nextGetIndex(int nb)
    {
        if (limit - position < nb) {
            throw new BufferUnderflowException();
        }
        int p = position;
        position += nb;
        return p;
    }

    protected int ix(int i)
    {
        return i + offset;
    }

    final int checkIndex(int i)
    {
        if ((i < 0) || (i >= limit)) {
            throw new IndexOutOfBoundsException();
        }
        return i;
    }

    private byte readByte(long position) throws IOException
    {
        byte[] buffer = new byte[1];
        is.readFully(position, buffer);
        return buffer[0];
    }

    private int readInt(long position) throws IOException
    {
        byte[] buffer = new byte[4];
        is.readFully(position, buffer);
        return makeInt(buffer[0], buffer[1], buffer[2], buffer[3]);
    }

    private long readLong(long position) throws IOException
    {
        byte[] buffer = new byte[8];
        is.readFully(position, buffer);
        return makeLong(buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                buffer[7]);
    }

    final int checkIndex(int i, int nb)
    {
        if ((i < 0) || (nb > limit - i)) {
            throw new IndexOutOfBoundsException();
        }
        return i;
    }

    static void checkBounds(int off, int len, int size)
    { // package-private
        if ((off | len | (off + len) | (size - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }
    }

    private static int makeInt(byte b3, byte b2, byte b1, byte b0)
    {
        return (((b3) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) << 8) |
                ((b0 & 0xff)));
    }

    private static long makeLong(byte b7, byte b6, byte b5, byte b4, byte b3, byte b2, byte b1,
            byte b0)
    {
        return ((((long) b7) << 56) |
                (((long) b6 & 0xff) << 48) |
                (((long) b5 & 0xff) << 40) |
                (((long) b4 & 0xff) << 32) |
                (((long) b3 & 0xff) << 24) |
                (((long) b2 & 0xff) << 16) |
                (((long) b1 & 0xff) << 8) |
                (((long) b0 & 0xff)));
    }
}
