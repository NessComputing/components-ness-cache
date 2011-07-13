package ness.cache;

import java.util.Collections;

import javax.annotation.CheckForNull;

import org.joda.time.DateTime;

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

    /**
     * Set a cache entry with a given value and expiration date.  Note that the value byte array
     * is shared, and the cache infrastructure assumes that it owns the passed in byte array.
     * @see Cache#set(String, java.util.Map)
     */
    public void set(String key, byte[] value, DateTime expiry) {
        cache.set(namespace, Collections.singletonMap(key, CacheStore.fromSharedBytes(value, expiry)));
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
     * Clear a single key
     * @see Cache#clear(String, java.util.Collection)
     */
    public void clear(String key) {
        cache.clear(namespace, Collections.singleton(key));
    }
}
