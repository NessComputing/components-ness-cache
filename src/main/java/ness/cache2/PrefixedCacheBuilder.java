package ness.cache2;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Types;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Duration;
import org.skife.config.TimeSpan;

public final class PrefixedCacheBuilder<P, K, V> implements Provider<PrefixedCache<P, K, V>>
{
    private final String namespace;

    private final TypeLiteral<P> prefixType;
    private final TypeLiteral<K> keyType;
    private final TypeLiteral<V> valueType;

    private Annotation annotation;
    private Class<? extends Annotation> serializerAnnotation = null;
    private Class<? extends Annotation> deserializerAnnotation = null;
    private Duration expiration;
    private Duration jitter;

    private NessCache nessCache = null;

    private Injector inj = null;
    private Function<Pair<P, K>, String> keySerializer = null;
    private Function<? super V, byte []> valueSerializer = null;
    private Function<byte [] , ? extends V> valueDeserializer = null;



    public static final <P, K, V> PrefixedCacheBuilder<P, K, V> buildPrefixCache(final String namespace,
                                                                                 final TypeLiteral<P> prefix,
                                                                                 final TypeLiteral<K> key,
                                                                                 final TypeLiteral<V> value)
    {
        return new PrefixedCacheBuilder<P, K, V>(namespace, prefix, key, value);
    }

    public static final <P, K, V> PrefixedCacheBuilder<P, K, V> buildPrefixCache(final String namespace,
                                                                                 final Class<P> prefix,
                                                                                 final Class<K> key,
                                                                                 final Class<V> value)
    {
        return new PrefixedCacheBuilder<P, K, V>(namespace, TypeLiteral.get(prefix), TypeLiteral.get(key), TypeLiteral.get(value));
    }


    private PrefixedCacheBuilder(final String namespace,
                                 final TypeLiteral<P> prefixType,
                                 final TypeLiteral<K> keyType,
                                 final TypeLiteral<V> valueType)
    {
        this.namespace = namespace;
        this.prefixType = prefixType;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public PrefixedCacheBuilder<P, K, V> withAnnotation(final String name)
    {
        this.annotation = Names.named(name);
        return this;
    }

    public PrefixedCacheBuilder<P, K, V> withAnnotation(final Annotation annotation)
    {
        this.annotation = annotation;
        return this;
    }

    public PrefixedCacheBuilder<P, K, V> withDeserializer(final Class<? extends Annotation> deserializerAnnotation)
    {
        this.deserializerAnnotation = deserializerAnnotation;
        return this;
    }

    public PrefixedCacheBuilder<P, K, V> withSerializer(final Class<? extends Annotation> serializerAnnotation)
    {
        this.serializerAnnotation = serializerAnnotation;
        return this;
    }

    public PrefixedCacheBuilder<P, K, V> withSerializerAndDeserializer(final Class<? extends Annotation> annotation)
    {
        this.serializerAnnotation = annotation;
        this.deserializerAnnotation = annotation;
        return this;
    }

    public PrefixedCacheBuilder<P, K, V> withExpirationAndJitter(final Duration expiration, final Duration jitter)
    {
        this.expiration = expiration;
        this.jitter = jitter;

        return this;
    }

    public PrefixedCacheBuilder<P, K, V> withExpirationAndJitter(final TimeSpan expiration, final TimeSpan jitter)
    {
        this.expiration = new Duration(expiration.getMillis());
        this.jitter = new Duration(jitter.getMillis());

        return this;
    }

    @Inject
    void setInjector(final Injector inj)
    {
        this.inj = inj;
    }

    @SuppressWarnings("unchecked")
    private void obnoxiousWorkaroundForGuiceBug()
    {
        final ParameterizedType pairType = Types.newParameterizedType(Pair.class, prefixType.getType(), keyType.getType());
        final ParameterizedType keySerType = Types.newParameterizedType(Function.class, pairType, String.class);
        final ParameterizedType valueSerType = Types.newParameterizedType(Function.class, Types.supertypeOf(valueType.getType()), byte[].class);
        final ParameterizedType valueDeserType = Types.newParameterizedType(Function.class, byte[].class, Types.subtypeOf(valueType.getType()));

        if (serializerAnnotation != null) {
            keySerializer = inj.getInstance((Key<Function<Pair<P, K>, String>>) Key.get(keySerType, serializerAnnotation));
            valueSerializer = inj.getInstance((Key<Function<? super V, byte []>>) Key.get(valueSerType, serializerAnnotation));
        }
        else {
            keySerializer = inj.getInstance((Key<Function<Pair<P, K>, String>>) Key.get(keySerType));
            valueSerializer = inj.getInstance((Key<Function<? super V, byte []>>) Key.get(valueSerType));
        }

        if (deserializerAnnotation != null) {
            valueDeserializer = inj.getInstance((Key<Function<byte [] , ? extends V>>)Key.get(valueDeserType, deserializerAnnotation));
        }
        else {
            valueDeserializer = inj.getInstance((Key<Function<byte [] , ? extends V>>)Key.get(valueDeserType));
        }

        if (annotation == null) {
            nessCache = inj.getInstance(NessCache.class);
        }
        else {
            nessCache = inj.getInstance(Key.get(NessCache.class, annotation));
        }
    }

    @Override
    public PrefixedCache<P, K, V> get()
    {
        obnoxiousWorkaroundForGuiceBug();

        Preconditions.checkNotNull(nessCache, "No cache present!");
        Preconditions.checkNotNull(keySerializer, "No key serializer present!");
        Preconditions.checkNotNull(valueSerializer, "No value serializer present!");
        Preconditions.checkNotNull(valueDeserializer, "No value deserializer present!");

        return new PrefixedCache<P, K, V>(nessCache, namespace, expiration, jitter, keySerializer, valueSerializer, valueDeserializer);
    }
}
