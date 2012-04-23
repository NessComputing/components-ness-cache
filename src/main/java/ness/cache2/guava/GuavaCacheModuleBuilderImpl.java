package ness.cache2.guava;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;

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
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import com.google.inject.util.Types;

class GuavaCacheModuleBuilderImpl<K, V> implements GuavaCacheModuleBuilder<K, V> {
    private String cacheName;
    private String namespace;
    private TypeLiteral<K> kClass;
    private TypeLiteral<V> vClass;
    private Key<? extends Function<? super K, String>> keySerializerKey;
    private Key<? extends Function<? super V, byte[]>> valueSerializerKey;
    private Key<? extends Function<byte[], ? extends V>> valueDeserializerKey;
    private Function<? super K, String> keySerializerFunction;
    private Function<? super V, byte[]> valueSerializerFunction;
    private Function<byte[], ? extends V> valueDeserializerFunction;
    private Duration expiry;
    private Duration expiryJitter;

    private Key<? extends CacheLoader<? super K, ? extends V>> loaderKey;

    GuavaCacheModuleBuilderImpl(String cacheName, String namespace, TypeLiteral<K> kClass, TypeLiteral<V> vClass) {
        this.cacheName = cacheName;
        this.namespace = namespace;

        this.kClass = kClass;
        this.vClass = vClass;

        withSerializers();
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withSerializers() {
        return withKeySerializer().withValueSerializer();
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withSerializers(Annotation bindingAnnotation) {
        return withKeySerializer(bindingAnnotation).withValueSerializer(bindingAnnotation);
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withSerializers(Class<? extends Annotation> bindingAnnotationClass) {
        return withKeySerializer(bindingAnnotationClass).withValueSerializer(bindingAnnotationClass);
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withKeySerializer(Key<? extends Function<? super K, String>> newKeySerializerKey) {
        keySerializerKey = newKeySerializerKey;
        keySerializerFunction = null;
        return this;
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withKeySerializer() {
        ParameterizedType functionType = Types.newParameterizedType(
                Function.class,
                Types.supertypeOf(kClass.getType()),
                String.class);

        @SuppressWarnings("unchecked")
        final Key<? extends Function<? super K, String>> key =
            (Key<? extends Function<? super K, String>>) Key.get(functionType);

        return withKeySerializer(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public GuavaCacheModuleBuilder<K, V> withKeySerializer(Annotation bindingAnnotation) {
        ParameterizedType functionType = Types.newParameterizedType(
                Function.class,
                Types.supertypeOf(kClass.getType()),
                String.class);

        Key<? extends Function<? super K, String>> key =
            (Key<? extends Function<? super K, String>>) Key.get(functionType, bindingAnnotation);

        return withKeySerializer(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public GuavaCacheModuleBuilder<K, V> withKeySerializer(Class<? extends Annotation> bindingAnnotationClass) {
        return withKeySerializer(
                (Key<? extends Function<? super K, String>>) Key.get(
                        Types.newParameterizedType(
                                Function.class,
                                Types.supertypeOf(kClass.getType()),
                                String.class),
                        bindingAnnotationClass));
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withKeySerializer(Function<? super K, String> keySerializerFunction) {
        this.keySerializerFunction = keySerializerFunction;
        return this;
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withValueSerializer() {
        ParameterizedType serializerType = Types.newParameterizedType(
                Function.class,
                Types.supertypeOf(vClass.getType()),
                byte[].class);

        ParameterizedType deserializerType = Types.newParameterizedType(
                Function.class,
                byte[].class,
                Types.subtypeOf(vClass.getType()));

        @SuppressWarnings("unchecked")
        final Key<? extends Function<? super V, byte[]>> serializerKey =
            (Key<? extends Function<? super V, byte[]>>) Key.get(serializerType);

        @SuppressWarnings("unchecked")
        final Key<? extends Function<byte[], ? extends V>> deserializerKey =
            (Key<? extends Function<byte[], ? extends V>>) Key.get(deserializerType);

        return withValueSerializer(serializerKey, deserializerKey);
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withValueSerializer(Annotation bindingAnnotation) {
        ParameterizedType serializerType = Types.newParameterizedType(
                Function.class,
                Types.supertypeOf(vClass.getType()),
                byte[].class);
        ParameterizedType deserializerType = Types.newParameterizedType(
                Function.class,
                byte[].class,
                Types.subtypeOf(vClass.getType()));

        @SuppressWarnings("unchecked")
        Key<? extends Function<? super V, byte[]>> serializerKey =
            (Key<? extends Function<? super V, byte[]>>) Key.get(serializerType, bindingAnnotation);

        @SuppressWarnings("unchecked")
        Key<? extends Function<byte[], ? extends V>> deserializerKey =
            (Key<? extends Function<byte[], ? extends V>>) Key.get(deserializerType, bindingAnnotation);

        return withValueSerializer(serializerKey, deserializerKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public GuavaCacheModuleBuilder<K, V> withValueSerializer(Class<? extends Annotation> bindingAnnotationClass) {
        return withValueSerializer(
                (Key<? extends Function<? super V, byte[]>>) Key.get(
                        Types.newParameterizedType(
                                Function.class,
                                Types.supertypeOf(vClass.getType()),
                                byte[].class),
                        bindingAnnotationClass),
                (Key<? extends Function<byte[], ? extends V>>) Key.get(
                        Types.newParameterizedType(
                                Function.class,
                                byte[].class,
                                Types.subtypeOf(vClass.getType())),
                        bindingAnnotationClass));
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withValueSerializer(
            Key<? extends Function<? super V, byte[]>> newValueSerializerKey,
            Key<? extends Function<byte[], ? extends V>> newValueDeserializerKey) {
        this.valueSerializerKey = newValueSerializerKey;
        this.valueDeserializerKey = newValueDeserializerKey;
        this.valueSerializerFunction = null;
        this.valueDeserializerFunction = null;
        return this;
    }

    @Override
    public GuavaCacheModuleBuilder<K, V> withValueSerializer(
            Function<? super V, byte[]> valueSerializerFunction,
            Function<byte[], ? extends V> valueDeserializerFunction) {
        this.valueSerializerFunction = valueSerializerFunction;
        this.valueDeserializerFunction = valueDeserializerFunction;
        return this;
    }

    @Override
    public GuavaCacheModuleBuilderImpl<K, V> withExpiration(Duration expiry) {
        return withExpiration(expiry, null);
    }

    @Override
    public GuavaCacheModuleBuilderImpl<K, V> withExpiration(Duration expiry, Duration expiryJitter) {
        this.expiry = expiry;
        this.expiryJitter = expiryJitter;
        return this;
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
            @SuppressWarnings("unchecked")
            @Override
            protected void configure() {
                loaderKey = (Key<? extends CacheLoader<? super K, ? extends V>>) Key.get(
                        Types.newParameterizedType(
                                CacheLoader.class,
                                Types.supertypeOf(kClass.getType()),
                                Types.subtypeOf(vClass.getType())));

                bind ((Key<CacheLoader<? super K, ? extends V>>) loaderKey).toProvider(cacheLoaderProvider);
                install (build (loaderKey));
            }
        };
    }

    @Override
    public Module build(final Key<? extends CacheLoader<? super K, ? extends V>> key) {
        this.loaderKey = key;

        Preconditions.checkState(keySerializerKey != null, "somehow you got a null key serializer key?");

        Preconditions.checkState(valueSerializerKey != null, "somehow you got a null value serializer key?");
        Preconditions.checkState(valueDeserializerKey != null, "somehow you got a null value deserializer key?");

        return new AbstractModule() {
            @SuppressWarnings("unchecked")
            @Override
            protected void configure() {
                CacheProvider provider = new CacheProvider();

                bind ((Key<com.google.common.cache.Cache<K, V>>) Key.get(
                        Types.newParameterizedType(com.google.common.cache.Cache.class, kClass.getType(), vClass.getType()),
                        Names.named(namespace))).toProvider(provider).in(Scopes.SINGLETON);

                if (loaderKey != null) {
                    bind ((Key<LoadingCache<K, V>>) Key.get(
                            Types.newParameterizedType(LoadingCache.class, kClass.getType(), vClass.getType()),
                            Names.named(namespace))).toProvider(provider).in(Scopes.SINGLETON);
                }

                if (keySerializerFunction != null) {
                    bind ((Key<Function<? super K, String>>) keySerializerKey).toInstance(keySerializerFunction);
                }
                if (valueSerializerFunction != null) {
                    bind ((Key<Function<? super V, byte[]>>) valueSerializerKey).toInstance(valueSerializerFunction);
                }
                if (valueDeserializerFunction != null) {
                    bind ((Key<Function<byte[], ? extends V>>) valueDeserializerKey).toInstance(valueDeserializerFunction);
                }
            }
        };
    }

    @Singleton
    class CacheProvider implements Provider<LoadingCache<K, V>> {

        private Injector injector;

        CacheProvider() {
        }

        @Inject
        public void setInjector(Injector injector) {
            this.injector = injector;
        }

        @Override
        public LoadingCache<K, V> get() {
            Preconditions.checkState(injector != null, "injector never got injected!");

            NamespacedCache cache = injector.getInstance(Key.get(NessCache.class, Names.named(cacheName))).withNamespace(namespace);

            CacheLoader<? super K, ? extends V> cacheLoader = null;

            if (loaderKey != null) {
                cacheLoader = injector.getInstance(loaderKey);
            }

            Function<? super K, String> keySerializerImpl = injector.getInstance(keySerializerKey);
            Function<? super V, byte[]> valueSerializerImpl = injector.getInstance(valueSerializerKey);
            Function<byte[], ? extends V> valueDeserializerImpl = injector.getInstance(valueDeserializerKey);

            return new GuavaCacheAdapter<K, V>(cache, kClass, keySerializerImpl, valueSerializerImpl, valueDeserializerImpl, cacheLoader, expiry, expiryJitter);
        }
    }
}
