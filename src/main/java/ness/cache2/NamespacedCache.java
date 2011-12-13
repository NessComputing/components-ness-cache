package ness.cache2;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.lang.BooleanUtils;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * A facade over a {@link Cache} which has the namespace field
 * pre-filled.
 */
public class NamespacedCache {
    private final Cache cache;
    private final String namespace;

    NamespacedCache(Cache cache, String namespace) {
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
     * @see Cache#set(String, java.util.Map)
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
     * @see Cache#set(String, java.util.Map)
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
     * @see Cache#set(String, java.util.Map)
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
     * @see Cache#get(String, java.util.Collection)
     */
    @CheckForNull
    public byte[] get(String key) {
        return cache.get(namespace, Collections.singleton(key)).get(key);
    }

    /**
     * Simple bulk fetch.
     * @see Cache#get(String, Collection)
     */
    @Nonnull
    public Map<String, byte[]> get(Collection<String> keys) {
        return cache.get(namespace, keys);
    }

    /**
     * Clear a single key
     * @see Cache#clear(String, java.util.Collection)
     */
    public void clear(String key) {
        clear(Collections.singleton(key));
    }

    /**
     * Bulk clear
     * @see Cache#clear(String, Collection)
     */
    public void clear(Collection<String> keys) {
        cache.clear(namespace, keys);
    }
}
