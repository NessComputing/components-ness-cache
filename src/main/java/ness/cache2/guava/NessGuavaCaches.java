package ness.cache2.guava;

import com.google.common.base.Functions;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Providers;

public class NessGuavaCaches {

    private NessGuavaCaches() { }

    /**
     * Create the default cache module builder.  The default key type is String, and the default value type is byte[].
     * Serializers are provided unless the types change.
     *
     * @param cacheName the name associated with the CacheModule to be used
     * @param namespace the namespace to give to NamespacedCache
     */
    public static GuavaCacheModuleBuilder<String, byte[]> newModuleBuilder(String cacheName, String namespace) {
        return new GuavaCacheModuleBuilderImpl<String, byte[]>(
                cacheName,
                namespace,
                TypeLiteral.get(String.class),
                TypeLiteral.get(byte[].class),
                Providers.of(Functions.toStringFunction()),
                Providers.of(Functions.<byte[]>identity()),
                Providers.of(Functions.<byte[]>identity()),
                null,
                null);
    }
}
