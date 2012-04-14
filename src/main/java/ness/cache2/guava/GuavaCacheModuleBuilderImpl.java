package ness.cache2.guava;

import ness.cache2.NamespacedCache;
import ness.cache2.NessCache;

import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import com.google.inject.util.Types;

class GuavaCacheModuleBuilderImpl<K, V> implements GuavaCacheModuleBuilder<K, V> {
    private final String cacheName;
    private final String namespace;
    private final TypeLiteral<K> kClass;
    private final TypeLiteral<V> vClass;
    private final Provider<? extends Function<? super K, String>> keySerializer;
    private final Provider<? extends Function<? super V, byte[]>> valueSerializer;
    private final Provider<? extends Function<byte[], ? extends V>> valueDeserializer;
    private final Duration expiry;
    private final Duration expiryJitter;

    private volatile Key<CacheLoader<? super K, ? extends V>> loaderKey;
    private volatile Key<Function<? super K, String>> keySerializerKey;
    private volatile Key<Function<? super V, byte[]>> valueSerializerKey;
    private volatile Key<Function<byte[], ? extends V>> valueDeserializerKey;

    GuavaCacheModuleBuilderImpl(
            String cacheName,
            String namespace,
            TypeLiteral<K> kClass,
            TypeLiteral<V> vClass,
            Provider<? extends Function<? super K, String>> keySerializer,
            Provider<? extends Function<? super V, byte[]>> valueSerializer,
            Provider<? extends Function<byte[], ? extends V>> valueDeserializer,
            Duration expiry,
            Duration expiryJitter)
    {
        this.cacheName = cacheName;
        this.namespace = namespace;
        this.kClass = kClass;
        this.vClass = vClass;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
        this.expiry = expiry;
        this.expiryJitter = expiryJitter;
    }

    @Override
    public <NewK> GuavaCacheModuleBuilderImpl<NewK, V> withKeyType(Class<NewK> kClass)
    {
        return withKeyType(TypeLiteral.get(kClass));
    }

    @Override
    public <NewK> GuavaCacheModuleBuilderImpl<NewK, V> withKeyType(TypeLiteral<NewK> kClass)
    {
        return new GuavaCacheModuleBuilderImpl<NewK, V>(cacheName, namespace, kClass, vClass, null, valueSerializer, valueDeserializer, expiry, expiryJitter);
    }

    @Override
    public GuavaCacheModuleBuilderImpl<K, V> withKeySerializer(Function<? super K, String> keySerialization)
    {
        return new GuavaCacheModuleBuilderImpl<K, V>(cacheName, namespace, kClass, vClass, Providers.of(keySerialization), valueSerializer, valueDeserializer, expiry, expiryJitter);
    }

    @Override
    public <NewV> GuavaCacheModuleBuilderImpl<K, NewV> withValueType(Class<NewV> vClass)
    {
        return withValueType(TypeLiteral.get(vClass));
    }

    @Override
    public <NewV> GuavaCacheModuleBuilderImpl<K, NewV> withValueType(TypeLiteral<NewV> vClass)
    {
        return new GuavaCacheModuleBuilderImpl<K, NewV>(cacheName, namespace, kClass, vClass, keySerializer, null, null, expiry, expiryJitter);
    }

    @Override
    public GuavaCacheModuleBuilderImpl<K, V> withValueSerializer(
            Function<? super V, byte[]> valueSerializer,
            Function<byte[], ? extends V> valueDeserializer)
    {
        return new GuavaCacheModuleBuilderImpl<K, V>(cacheName, namespace, kClass, vClass, keySerializer, Providers.of(valueSerializer), Providers.of(valueDeserializer), expiry, expiryJitter);
    }

    @Override
    public GuavaCacheModuleBuilderImpl<K, V> withExpiration(Duration expiry) {
        return withExpiration(expiry, null);
    }

    @Override
    public GuavaCacheModuleBuilderImpl<K, V> withExpiration(Duration expiry, Duration expiryJitter) {
        return new GuavaCacheModuleBuilderImpl<K, V>(cacheName, namespace, kClass, vClass, keySerializer, valueSerializer, valueDeserializer, expiry, expiryJitter);
    }

    @Override
    public Module build() {
        return build((Key<CacheLoader<? super K, ? extends V>>) null);
    }

    @Override
    public Module build(final CacheLoader<? super K, ? extends V> cacheLoader) {
        return build(Providers.of(cacheLoader));
    }

    @Override
    public Module build(final Provider<? extends CacheLoader<? super K, ? extends V>> cacheLoaderProvider) {
        return new AbstractModule() {
            @Override
            protected void configure() {
                bind (loaderKey).toProvider(cacheLoaderProvider);
                install (build (loaderKey));
            }
        };
    }

    @Override
    public Module build(final Key<CacheLoader<? super K, ? extends V>> key) {
        this.loaderKey = key;

        return new AbstractModule() {
            @SuppressWarnings("unchecked")
            @Override
            protected void configure() {
                CacheProvider provider = new CacheProvider();

                bind ((Key<com.google.common.cache.Cache<K, V>>) Key.get(
                        Types.newParameterizedType(com.google.common.cache.Cache.class, kClass.getType(), vClass.getType()),
                        Names.named(namespace))).toProvider(provider).in(Scopes.SINGLETON);

                if (loaderKey != null) {
                    bind ((Key<com.google.common.cache.LoadingCache<K, V>>) Key.get(
                            Types.newParameterizedType(LoadingCache.class, kClass.getType(), vClass.getType()),
                            Names.named(namespace))).toProvider(provider).in(Scopes.SINGLETON);
                }

                keySerializerKey = (Key<Function<? super K, String>>) Key.get(
                        Types.newParameterizedType(
                                Function.class,
                                Types.supertypeOf(kClass.getType()),
                                String.class));

                valueSerializerKey = (Key<Function<? super V, byte[]>>) Key.get(
                        Types.newParameterizedType(
                                Function.class,
                                Types.supertypeOf(vClass.getType()),
                                byte[].class));

                valueDeserializerKey = (Key<Function<byte[], ? extends V>>) Key.get(
                        Types.newParameterizedType(
                                Function.class,
                                byte[].class,
                                Types.subtypeOf(vClass.getType())));

                if (keySerializer != null) {
                    bind (keySerializerKey).toProvider(keySerializer);
                }

                if (valueSerializer != null) {
                    bind (valueSerializerKey).toProvider(valueSerializer);
                }

                if (valueDeserializer != null) {
                    bind (valueDeserializerKey).toProvider(valueDeserializer);
                }
            }
        };
    }

    class CacheProvider implements Provider<LoadingCache<K, V>> {

        private NamespacedCache cache;
        private CacheLoader<? super K, ? extends V> cacheLoader;
        private Function<? super K, String> keySerializerImpl;
        private Function<? super V, byte[]> valueSerializerImpl;
        private Function<byte[], ? extends V> valueDeserializerImpl;

        private GuavaCacheAdapter<K, V> realCache;

        CacheProvider() {
        }

        @Inject
        void setCache(Injector injector) {
            this.cache = injector.getInstance(Key.get(NessCache.class, Names.named(cacheName))).withNamespace(namespace);
            this.keySerializerImpl = injector.getInstance(keySerializerKey);
            this.valueSerializerImpl = injector.getInstance(valueSerializerKey);
            this.valueDeserializerImpl = injector.getInstance(valueDeserializerKey);

            if (loaderKey != null) {
                this.cacheLoader = injector.getInstance(loaderKey);
            }

            realCache = new GuavaCacheAdapter<K, V>(cache, kClass, keySerializerImpl, valueSerializerImpl, valueDeserializerImpl, cacheLoader, expiry, expiryJitter);
        }

        @Override
        public LoadingCache<K, V> get() {
            Preconditions.checkState(realCache != null, "provider never got injected!");
            return realCache;
        }
    }
}
