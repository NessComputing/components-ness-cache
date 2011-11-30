package ness.cache2;

import java.util.Collection;
import java.util.Map;

/**
 * Basic operations a cache backend must provide.
 */
interface InternalCacheProvider {

    /**
     * In a given namespace, store (add or overwrite) a collection of keys and corresponding values
     */
    void set(String namespace, Collection<CacheStore<byte []>> stores);

    /**
     * Bulk fetch a collection of keys
     */
    Map<String, byte[]> get(String namespace, Collection<String> keys);

    /**
     * Remove a collection of keys
     */
    void clear(String namespace, Collection<String> keys);

    /**
     * Try to add a collection of keys and corresponding values. Returns a map of boolean, true means that the operation was successful.
     *
     * This is an optional operation.
     */
    Map<String, Boolean> add(String namespace, Collection<CacheStore<byte []>> stores);

    /**
     * Increment a number of keys by the values provided in the cache store objects.
     *
     * This is an optional operation.
     *
     * @See {@link CacheStores#forKeys(Collection, int, org.joda.time.DateTime)}.
     */
    Map<String, Long> incr(String namespace, Collection<CacheStore<Integer>> stores);

    /**
     * Increment a number of keys by the values provided in the cache store objects.
     *
     * This is an optional operation.
     *
     * @See {@link CacheStores#forKeys(Collection, int, org.joda.time.DateTime)}.
     */
    Map<String, Long> decr(String namespace, Collection<CacheStore<Integer>> stores);
}
