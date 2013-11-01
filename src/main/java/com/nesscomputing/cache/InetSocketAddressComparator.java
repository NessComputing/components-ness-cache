/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nesscomputing.cache;

import java.net.InetSocketAddress;
import java.util.Comparator;

import com.google.common.base.Preconditions;

class InetSocketAddressComparator implements Comparator<InetSocketAddress>
{
    public static final InetSocketAddressComparator DEFAULT = new InetSocketAddressComparator();

    private InetSocketAddressComparator()
    {
    }

    @Override
    public int compare(final InetSocketAddress o1, final InetSocketAddress o2)
    {
        Preconditions.checkNotNull(o1);
        Preconditions.checkNotNull(o2);
        return o1.toString().compareTo(o2.toString()); // Yeah, it is cheap.
    }
}
