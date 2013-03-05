package ness.cache2;

import java.util.Collection;
import java.util.Map;


/**
 * Provides a provider-neutral caching layer which has asynchronous writes and synchronous
 * reads.  The API does not promise any sorts of consistency, although the provider may
 * be configured (via {@link CacheConfiguration}) to provide various guarantees.
 */
public interface NessCache {
    /**
     * Provide a view of this cache which automatically has the given namespace filled in.  Intended to be used
     * in constructors, e.g.
     * <pre>
     * private final NamespacedCache myCache;
     * \u0040Inject
     * MyCoolClass(Cache cache) {
     *     myCache = cache.withNamespace("extraCoolWithCheese");
     * }
     * </pre>
     */
    public NamespacedCache withNamespace(String namespace);

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
}
