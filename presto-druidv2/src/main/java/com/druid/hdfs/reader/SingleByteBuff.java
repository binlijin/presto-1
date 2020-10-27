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

import com.druid.hdfs.reader.utils.ByteBuff;

import java.nio.ByteBuffer;

public class SingleByteBuff
        extends ByteBuff
{
    // Underlying BB
    private final ByteBuffer buf;

    public SingleByteBuff(ByteBuffer buf)
    {
        this.buf = buf;
    }

    @Override public int position()
    {
        return this.buf.position();
    }

    @Override public SingleByteBuff position(int position)
    {
        this.buf.position(position);
        return this;
    }

    @Override public SingleByteBuff skip(int len)
    {
        this.buf.position(this.buf.position() + len);
        return this;
    }

    @Override public SingleByteBuff moveBack(int len)
    {
        this.buf.position(this.buf.position() - len);
        return this;
    }

    @Override public int capacity()
    {
        return this.buf.capacity();
    }

    @Override public int limit()
    {
        return this.buf.limit();
    }

    @Override public SingleByteBuff limit(int limit)
    {
        this.buf.limit(limit);
        return this;
    }

    @Override public SingleByteBuff rewind()
    {
        this.buf.rewind();
        return this;
    }

    @Override public SingleByteBuff mark()
    {
        this.buf.mark();
        return this;
    }

    @Override public int remaining()
    {
        return this.buf.remaining();
    }

    @Override public boolean hasRemaining()
    {
        return buf.hasRemaining();
    }

    @Override public SingleByteBuff reset()
    {
        this.buf.reset();
        return this;
    }

    @Override public SingleByteBuff slice()
    {
        return new SingleByteBuff(this.buf.slice());
    }

    @Override public SingleByteBuff duplicate()
    {
        return new SingleByteBuff(this.buf.duplicate());
    }

    @Override public byte get()
    {
        return buf.get();
    }

    @Override public byte get(int index)
    {
        return this.buf.get(index);
    }

    @Override public byte getByteAfterPosition(int offset)
    {
        return get(this.buf.position() + offset);
    }

    @Override public SingleByteBuff put(byte b)
    {
        this.buf.put(b);
        return this;
    }

    @Override public SingleByteBuff put(int index, byte b)
    {
        buf.put(index, b);
        return this;
    }

    @Override public void get(byte[] dst, int offset, int length)
    {
        ByteBuffer inDup = this.buf.duplicate();
        inDup.position(buf.position());
        inDup.get(dst, offset, length);
        buf.position(buf.position() + length);
    }

    @Override public void get(int sourceOffset, byte[] dst, int offset, int length)
    {
        ByteBuffer inDup = this.buf.duplicate();
        inDup.position(sourceOffset);
        inDup.get(dst, offset, length);
    }

    @Override public void get(byte[] dst)
    {
        get(dst, 0, dst.length);
    }

    @Override public SingleByteBuff put(int offset, ByteBuff src, int srcOffset, int length)
    {
        ByteBuffer outDup = ((SingleByteBuff) src).buf.duplicate();
        outDup.position(offset);
        ByteBuffer inDup = this.buf.duplicate();
        inDup.position(srcOffset).limit(srcOffset + length);
        outDup.put(inDup);
        return this;
    }

    @Override public SingleByteBuff put(byte[] src, int offset, int length)
    {
        this.buf.put(src, offset, length);
        return this;
    }

    @Override public SingleByteBuff put(byte[] src)
    {
        return put(src, 0, src.length);
    }

    @Override public boolean hasArray()
    {
        return this.buf.hasArray();
    }

    @Override public byte[] array()
    {
        return this.buf.array();
    }

    @Override public int arrayOffset()
    {
        return this.buf.arrayOffset();
    }

    @Override public short getShort()
    {
        return this.buf.getShort();
    }

    @Override public short getShort(int index)
    {
        return this.buf.getShort(index);
    }

    @Override public short getShortAfterPosition(int offset)
    {
        return getShort(this.buf.position() + offset);
    }

    @Override public int getInt()
    {
        return this.buf.getInt();
    }

    @Override public SingleByteBuff putInt(int value)
    {
        this.buf.putInt(value);
        return this;
    }

    @Override public int getInt(int index)
    {
        return this.buf.getInt(index);
    }

    @Override public int getIntAfterPosition(int offset)
    {
        return getInt(this.buf.position() + offset);
    }

    @Override public long getLong()
    {
        return this.buf.getLong();
    }

    @Override public SingleByteBuff putLong(long value)
    {
        this.buf.putLong(value);
        return this;
    }

    @Override public long getLong(int index)
    {
        return this.buf.getLong(index);
    }

    @Override public long getLongAfterPosition(int offset)
    {
        return getLong(this.buf.position() + offset);
    }

    @Override public byte[] toBytes(int offset, int length)
    {
        byte[] output = new byte[length];
        ByteBuffer inDup = buf.duplicate();
        inDup.position(offset);
        inDup.get(output, 0, length);
        return output;
    }

    @Override public void get(ByteBuffer out, int sourceOffset, int length)
    {
        ByteBuffer outDup = out.duplicate();
        outDup.position(0);
        ByteBuffer inDup = buf.duplicate();
        inDup.position(sourceOffset).limit(sourceOffset + length);
        outDup.put(inDup);
    }

    @Override public boolean equals(Object obj)
    {
        if (!(obj instanceof SingleByteBuff)) {
            return false;
        }
        return this.buf.equals(((SingleByteBuff) obj).buf);
    }

    @Override public int hashCode()
    {
        return this.buf.hashCode();
    }

    /**
     * @return the ByteBuffer which this wraps.
     */
    public ByteBuffer getEnclosingByteBuffer()
    {
        return this.buf;
    }
}
