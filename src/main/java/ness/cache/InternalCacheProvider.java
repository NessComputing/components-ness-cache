package ness.cache;

import java.util.Collection;
import java.util.Map;

/**
 * Basic operations a cache backend must provide.
 */
interface InternalCacheProvider {

    /**
     * In a given namespace, store (add or overwrite) a collection of keys and corresponding values
     */
    void set(String namespace, Map<String, CacheStore> stores);

    /**
     * Bulk fetch a collection of keys
     */
    Map<String, byte[]> get(String namespace, Collection<String> keys);

    /**
     * Remove a collection of keys
     */
    void clear(String namespace, Collection<String> keys);
}
