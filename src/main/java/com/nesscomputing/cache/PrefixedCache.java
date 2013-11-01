/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nesscomputing.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.Duration;


public class PrefixedCache<P, K, V>
{
    private final String namespace;
    private final Duration expiration;
    private final long jitter;

    private final Random random = new Random();

    private final Function<Pair<P, K>, String> keySerializer;
    private final Function<? super V, byte []> valueSerializer;
    private final Function<byte [] , ? extends V> valueDeserializer;

    private final NessCache nessCache;

    public PrefixedCache(final NessCache nessCache,
                         final String namespace,
                         final Duration expiration,
                         final Duration jitter,
                         final Function<Pair<P, K>, String> keySerializer,
                         final Function<? super V, byte []> valueSerializer,
                         final Function<byte [] , ? extends V> valueDeserializer)
    {
        Preconditions.checkNotNull(namespace, "the namespace must not be null!");
        this.nessCache = nessCache;
        this.namespace = namespace;
        this.expiration = expiration;

        if (expiration != null) {
            Preconditions.checkNotNull(jitter, "the jitter value must not be null!");
            this.jitter = jitter.getMillis();
        }
        else {
            this.jitter = 0;
        }

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
    }


    public void put(final P prefix, final K key, final V value)
    {
        final String keyString = keySerializer.apply(SerializablePair.of(prefix, key));
        final byte [] valueBytes = valueSerializer.apply(value);
        nessCache.set(namespace, Collections.singleton(CacheStores.fromSharedBytes(keyString, valueBytes, getExpiry())));
    }

    public boolean add(final P prefix, final K key, final V value)
    {
        final String keyString = keySerializer.apply(SerializablePair.of(prefix, key));
        final byte [] valueBytes = valueSerializer.apply(value);
        return BooleanUtils.toBoolean(nessCache.add(namespace, Collections.singleton(CacheStores.fromSharedBytes(keyString, valueBytes, getExpiry()))).get(key));
    }

    public void putAll(final P prefix, final Map<K, ? extends V> entries)
    {
        nessCache.set(namespace, Collections2.transform(entries.entrySet(), new Function<Map.Entry<K, ? extends V>, CacheStore<byte []>>() {
                    @Override
                    public CacheStore<byte[]> apply(final Map.Entry<K, ? extends V> entry) {
                        final String keyString = keySerializer.apply(SerializablePair.of(prefix, entry.getKey()));
                        final byte [] valueBytes = valueSerializer.apply(entry.getValue());
                        return CacheStores.fromSharedBytes(keyString, valueBytes, getExpiry());
                    }
                }));
    }

    public final Map<K, Boolean> addAll(final P prefix, final Map<K, ? extends V> entries)
    {
        final Function<K, String> prefixFunction = new PrefixFunction<P, K>(prefix, keySerializer);
        final Map<String, K> keyStrings = Maps.uniqueIndex(entries.keySet(), prefixFunction);

        final ImmutableMap.Builder<K, Boolean> builder = ImmutableMap.builder();

        final Map<String, Boolean> res = nessCache.add(namespace, Collections2.transform(entries.entrySet(), new Function<Map.Entry<K, ? extends V>, CacheStore<byte []>>() {
                    @Override
                    public CacheStore<byte[]> apply(final Map.Entry<K, ? extends V> entry) {
                        final String keyString = keySerializer.apply(SerializablePair.of(prefix, entry.getKey()));
                        final byte [] valueBytes = valueSerializer.apply(entry.getValue());
                        return CacheStores.fromSharedBytes(keyString, valueBytes, getExpiry());
                    }
                }));

        for (Map.Entry<String, Boolean> result : res.entrySet()) {
            builder.put(keyStrings.get(result.getKey()), result.getValue());
        }

        return builder.build();
    }

    @CheckForNull
    public V get(final P prefix, final K key)
    {
        final String keyString = keySerializer.apply(SerializablePair.of(prefix, key));
        final byte [] result = nessCache.get(namespace, Collections.singleton(keyString)).get(keyString);
        return result == null ? null : valueDeserializer.apply(result);
    }

    @Nonnull
    public Map<K, V> get(P prefix, Collection<? extends K> keys)
    {
        final Function<K, String> prefixFunction = new PrefixFunction<P, K>(prefix, keySerializer);
        final Map<String, ? extends K> keyStrings = Maps.uniqueIndex(keys, prefixFunction);
        final Map<String, byte []> res = nessCache.get(namespace, keyStrings.keySet());

        final ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();

        for (final Map.Entry<String, byte[]> entry : res.entrySet()) {
            final K key = keyStrings.get(entry.getKey());
            try {
                final byte [] result = entry.getValue();
                if (result != null) {
                    builder.put(key, valueDeserializer.apply(result));
                }
            }
            catch (Exception e) {
                clear(prefix, key);
                throw Throwables.propagate(e);
            }
        }

        return builder.build();
    }

    public void clear(final P prefix, final K key)
    {
        clear(prefix, Collections.singleton(key));
    }

    public void clear(final P prefix, final Collection<? extends K> keys)
    {
        final Function<K, String> prefixFunction = new PrefixFunction<P, K>(prefix, keySerializer);
        nessCache.clear(namespace, Collections2.transform(keys, prefixFunction));
    }

    private DateTime getExpiry()
    {
        if (expiration == null) {
            return DateTime.now().plusYears(10);
        }

        DateTime result = DateTime.now().plus(expiration);

        // 0..1 -> 0..jitter*2 - jitter -> -jitter .. +jitter
        return result.plus((long)(random.nextFloat() * 2.0 * jitter) - jitter);
    }

    private static class PrefixFunction<KeyPrefix, KeyValue> implements Function<KeyValue, String>
    {
        private final KeyPrefix prefix;
        private final Function<Pair<KeyPrefix, KeyValue>, String> serializer;

        PrefixFunction(final KeyPrefix prefix, final Function<Pair<KeyPrefix, KeyValue>, String> serializer)
        {
            this.prefix = prefix;
            this.serializer = serializer;
        }

        @Override
        public String apply(final KeyValue key)
        {
            Preconditions.checkState(key != null, "the key must not be null!");

            return serializer.apply(SerializablePair.of(prefix, key));
        }
    }

    public static final class SerializablePair<A, B> extends Pair<A, B>
    {
        private static final long serialVersionUID = 1L;

        private final A a;
        private final B b;

        public static <A, B> SerializablePair<A, B> of(final A a, final B b)
        {
            return new SerializablePair<A, B>(a, b);
        }

        SerializablePair(@JsonProperty("key") final A a,
                         @JsonProperty("value") final B b)
        {
            this.a = a;
            this.b = b;
        }

        @Override
        public A getLeft()
        {
            return a;
        }

        @Override
        public B getRight()
        {
            return b;
        }

        @Override
        public B setValue(B value)
        {
            throw new UnsupportedOperationException();
        }

        @JsonValue
        public String getCacheKey()
        {
            return a + "|" + b;
        }
    }
}
