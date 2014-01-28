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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import org.apache.commons.lang3.BooleanUtils;
import org.joda.time.DateTime;

/**
 * A facade over a {@link NessCache} which has the namespace field
 * pre-filled.
 */
public class NamespacedCache {
    private final NessCache cache;
    private final String namespace;

    NamespacedCache(NessCache cache, String namespace) {
        this.cache = cache;
        this.namespace = namespace;
    }

    public String getNamespace()
    {
        return namespace;
    }

    /**
     * Set a cache entry with a given value and expiration date.  Note that the value byte array
     * is shared, and the cache infrastructure assumes that it owns the passed in byte array.
     * @see NessCache#set(String, Collection)
     */
    public void set(String key, byte[] value, DateTime expiry) {
        cache.set(namespace, Collections.singleton(CacheStores.fromSharedBytes(key, value, expiry)));
    }

    /**
     * Tries to add a cache entry if it does not already exist.
     *
     *  This is an optional operation.
     */
    public boolean add(String key, byte[] value, DateTime expiry)
    {
        return BooleanUtils.toBoolean(cache.add(namespace, Collections.singleton(CacheStores.fromSharedBytes(key, value, expiry))).get(key));
    }

    /**
     * Set many cache entries with given values and expiration date.  Note that the value byte array
     * is shared, and the cache infrastructure assumes that it owns the passed in byte array.
     * @see NessCache#set(String, Collection)
     */
    public void set(Map<String, byte[]> entries, final DateTime expiry) {
        cache.set(namespace, Collections2.transform(entries.entrySet(), new Function<Map.Entry<String, byte []>, CacheStore<byte []>>() {
            @Override
            public CacheStore<byte[]> apply(final Entry<String, byte[]> entry) {
                return CacheStores.fromSharedBytes(entry.getKey(), entry.getValue(), expiry);
            }
        }));
    }


    /**
     * Add many cache entries with given values and expiration date.  Note that the value byte array
     * is shared, and the cache infrastructure assumes that it owns the passed in byte array.
     * @see NessCache#set(String, Collection)
     */
    public Map<String, Boolean> add(Map<String, byte[]> entries, final DateTime expiry) {
        return cache.add(namespace, Collections2.transform(entries.entrySet(), new Function<Map.Entry<String, byte []>, CacheStore<byte []>>() {
            @Override
            public CacheStore<byte[]> apply(final Entry<String, byte[]> entry) {
                return CacheStores.fromSharedBytes(entry.getKey(), entry.getValue(), expiry);
            }
        }));
    }

    /**
     * Single key fetch.  Returns null if no entry exists
     * @see NessCache#get(String, java.util.Collection)
     */
    @CheckForNull
    public byte[] get(String key) {
        return cache.get(namespace, Collections.singleton(key)).get(key);
    }

    /**
     * Simple bulk fetch.
     * @see NessCache#get(String, Collection)
     */
    @Nonnull
    public Map<String, byte[]> get(Collection<String> keys) {
        return cache.get(namespace, keys);
    }

    /**
     * Clear a single key
     * @see NessCache#clear(String, java.util.Collection)
     */
    public void clear(String key) {
        clear(Collections.singleton(key));
    }

    /**
     * Bulk clear
     * @see NessCache#clear(String, Collection)
     */
    public void clear(Collection<String> keys) {
        cache.clear(namespace, keys);
    }
}
