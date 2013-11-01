package com.nesscomputing.cache.guava;

import com.google.inject.TypeLiteral;

public class NessGuavaCaches {

    private NessGuavaCaches() { }

    /**
     * Create a cache builder
     *
     * @param cacheName the name associated with the CacheModule to be used
     * @param namespace the namespace to give to NamespacedCache
     */
    public static <K, V> GuavaCacheModuleBuilder<K, V> newModuleBuilder(String cacheName, String namespace, TypeLiteral<K> kType, TypeLiteral<V> vType) {
        return new GuavaCacheModuleBuilderImpl<K, V>(cacheName, namespace, kType, vType);
    }

    /**
     * Create a cache builder
     *
     * @param cacheName the name associated with the CacheModule to be used
     * @param namespace the namespace to give to NamespacedCache
     */
    public static <K, V> GuavaCacheModuleBuilder<K, V> newModuleBuilder(String cacheName, String namespace, Class<K> kType, Class<V> vType) {
        return newModuleBuilder(cacheName, namespace, TypeLiteral.get(kType), TypeLiteral.get(vType));
    }
}
