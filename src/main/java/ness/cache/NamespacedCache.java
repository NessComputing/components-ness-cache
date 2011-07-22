package ness.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.joda.time.DateTime;

import com.google.common.collect.Maps;

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
        cache.set(namespace, Collections.singletonMap(key, CacheStore.fromSharedBytes(value, expiry)));
    }

    /**
     * Set many cache entries with given values and expiration date.  Note that the value byte array
     * is shared, and the cache infrastructure assumes that it owns the passed in byte array.
     * @see Cache#set(String, java.util.Map)
     */
    public void set(Map<String, byte[]> entries, DateTime expiry) {
        Map<String, CacheStore> stores = Maps.newHashMap();

        for (Entry<String, byte[]> e : entries.entrySet()) {
            stores.put(e.getKey(), CacheStore.fromSharedBytes(e.getValue(), expiry));
        }

        cache.set(namespace, stores);
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
