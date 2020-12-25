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

import java.io.IOException;

/**
 * Exception thrown when an illegal argument is passed to a function/procedure.
 */
public class IllegalArgumentIOException
        extends IOException
{
    public IllegalArgumentIOException()
    {
        super();
    }

    public IllegalArgumentIOException(final String message)
    {
        super(message);
    }

    public IllegalArgumentIOException(final String message, final Throwable t)
    {
        super(message, t);
    }

    public IllegalArgumentIOException(final Throwable t)
    {
        super(t);
    }
}