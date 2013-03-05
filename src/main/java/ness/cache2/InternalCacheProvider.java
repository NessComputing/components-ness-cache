package ness.cache2;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Basic operations a cache backend must provide.
 */
interface InternalCacheProvider {

    /**
     * In a given namespace, store (add or overwrite) a collection of keys and corresponding values
     */
    void set(String namespace, Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics cacheStatistics);

    /**
     * Bulk fetch a collection of keys
     */
    Map<String, byte[]> get(String namespace, Collection<String> keys, @Nullable CacheStatistics cacheStatistics);

    /**
     * Remove a collection of keys
     */
    void clear(String namespace, Collection<String> keys, @Nullable CacheStatistics cacheStatistics);

    /**
     * Try to add a collection of keys and corresponding values. Returns a map of boolean, true means that the operation was successful.
     *
     * This is an optional operation.
     */
    Map<String, Boolean> add(String namespace, Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics cacheStatistics);
}
