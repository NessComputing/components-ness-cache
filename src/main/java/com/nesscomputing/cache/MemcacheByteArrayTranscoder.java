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

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

final class MemcacheByteArrayTranscoder implements Transcoder<byte[]> {
    @Override
    public boolean asyncDecode(CachedData d) {
        return false;
    }

    @Override
    public CachedData encode(byte[] data) {
        // Apparently Memcache does not differentiate between a byte[0] and null,
        // so use the flag field to mark nonnull values
        return new CachedData(data == null ? 0 : 1, data, getMaxSize());
    }

    @Override
    public byte[] decode(CachedData d) {
        return d.getFlags() == 0 ? null : d.getData();
    }

    @Override
    public int getMaxSize() {
        return Integer.MAX_VALUE;
    }
}
