package ness.cache;

import java.util.Collections;

import org.joda.time.DateTime;

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
     */
    public void set(String key, byte[] value, DateTime expiry) {
        cache.set(namespace, Collections.singletonMap(key, CacheStore.fromSharedBytes(value, expiry)));
    }
    
    public byte[] get(String key) {
        return cache.get(namespace, Collections.singleton(key)).get(key);
    }
    
    public void clear(String key) {
        cache.clear(namespace, Collections.singleton(key));
    }
}
