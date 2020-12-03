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
package io.prestosql.cache.block.bucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class UnsafeAccess
{
    private static final Logger LOG = LoggerFactory.getLogger(UnsafeAccess.class);

    public static final Unsafe theUnsafe;

    /**
     * The offset to the first element in a byte array.
     */
    public static final long BYTE_ARRAY_BASE_OFFSET;

    public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder()
            .equals(ByteOrder.LITTLE_ENDIAN);

    // This number limits the number of bytes to copy per call to Unsafe's
    // copyMemory method. A limit is imposed to allow for safepoint polling
    // during a large copy
    static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    static {
        theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                try {
                    Field f = Unsafe.class.getDeclaredField("theUnsafe");
                    f.setAccessible(true);
                    return f.get(null);
                }
                catch (Throwable e) {
                    LOG.warn("sun.misc.Unsafe is not accessible", e);
                }
                return null;
            }
        });

        if (theUnsafe != null) {
            BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
        }
        else {
            BYTE_ARRAY_BASE_OFFSET = -1;
        }
    }

    private UnsafeAccess() {}

    // APIs to read primitive data from a byte[] using Unsafe way

    /**
     * Converts a byte array to a short value considering it was written in big-endian format.
     *
     * @param bytes byte array
     * @param offset offset into array
     * @return the short value
     */
    public static short toShort(byte[] bytes, int offset)
    {
        if (LITTLE_ENDIAN) {
            return Short.reverseBytes(theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
        }
        else {
            return theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
        }
    }

    /**
     * Converts a byte array to an int value considering it was written in big-endian format.
     *
     * @param bytes byte array
     * @param offset offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset)
    {
        if (LITTLE_ENDIAN) {
            return Integer.reverseBytes(theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
        }
        else {
            return theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
        }
    }

    /**
     * Converts a byte array to a long value considering it was written in big-endian format.
     *
     * @param bytes byte array
     * @param offset offset into array
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset)
    {
        if (LITTLE_ENDIAN) {
            return Long.reverseBytes(theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
        }
        else {
            return theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
        }
    }

    // APIs to write primitive data to a byte[] using Unsafe way

    /**
     * Put a short value out to the specified byte array position in big-endian format.
     *
     * @param bytes the byte array
     * @param offset position in the array
     * @param val short to write out
     * @return incremented offset
     */
    public static int putShort(byte[] bytes, int offset, short val)
    {
        if (LITTLE_ENDIAN) {
            val = Short.reverseBytes(val);
        }
        theUnsafe.putShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
        return offset + Bytes.SIZEOF_SHORT;
    }

    /**
     * Put an int value out to the specified byte array position in big-endian format.
     *
     * @param bytes the byte array
     * @param offset position in the array
     * @param val int to write out
     * @return incremented offset
     */
    public static int putInt(byte[] bytes, int offset, int val)
    {
        if (LITTLE_ENDIAN) {
            val = Integer.reverseBytes(val);
        }
        theUnsafe.putInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
        return offset + Bytes.SIZEOF_INT;
    }

    /**
     * Put a long value out to the specified byte array position in big-endian format.
     *
     * @param bytes the byte array
     * @param offset position in the array
     * @param val long to write out
     * @return incremented offset
     */
    public static int putLong(byte[] bytes, int offset, long val)
    {
        if (LITTLE_ENDIAN) {
            val = Long.reverseBytes(val);
        }
        theUnsafe.putLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
        return offset + Bytes.SIZEOF_LONG;
    }

    // APIs to read primitive data from a ByteBuffer using Unsafe way

    /**
     * Reads a short value at the given Object's offset considering it was written in big-endian
     * format.
     *
     * @param ref
     * @param offset
     * @return short value at offset
     */
    public static short toShort(Object ref, long offset)
    {
        if (LITTLE_ENDIAN) {
            return Short.reverseBytes(theUnsafe.getShort(ref, offset));
        }
        return theUnsafe.getShort(ref, offset);
    }

    /**
     * Reads a int value at the given Object's offset considering it was written in big-endian
     * format.
     *
     * @param ref
     * @param offset
     * @return int value at offset
     */
    public static int toInt(Object ref, long offset)
    {
        if (LITTLE_ENDIAN) {
            return Integer.reverseBytes(theUnsafe.getInt(ref, offset));
        }
        return theUnsafe.getInt(ref, offset);
    }

    /**
     * Reads a long value at the given Object's offset considering it was written in big-endian
     * format.
     *
     * @param ref
     * @param offset
     * @return long value at offset
     */
    public static long toLong(Object ref, long offset)
    {
        if (LITTLE_ENDIAN) {
            return Long.reverseBytes(theUnsafe.getLong(ref, offset));
        }
        return theUnsafe.getLong(ref, offset);
    }

    private static void unsafeCopy(Object src, long srcAddr, Object dst, long destAddr, long len)
    {
        while (len > 0) {
            long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
            theUnsafe.copyMemory(src, srcAddr, dst, destAddr, size);
            len -= size;
            srcAddr += size;
            destAddr += size;
        }
    }
}
