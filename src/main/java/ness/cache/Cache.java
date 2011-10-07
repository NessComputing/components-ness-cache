package ness.cache;


/**
 * Provides a provider-neutral caching layer which has asynchronous writes and synchronous
 * reads.  The API does not promise any sorts of consistency, although the provider may
 * be configured (via {@link CacheConfiguration}) to provide various guarantees.
 */
public interface Cache extends InternalCacheProvider {
    /**
     * Provide a view of this cache which automatically has the given namespace filled in.  Intended to be used
     * in constructors, e.g.
     * <pre>
     * private final NamespacedCache myCache;
     * @Inject
     * MyCoolClass(Cache cache) {
     *     myCache = cache.withNamespace("extraCoolWithCheese");
     * }
     * </pre>
     */
    public NamespacedCache withNamespace(String namespace);

}
