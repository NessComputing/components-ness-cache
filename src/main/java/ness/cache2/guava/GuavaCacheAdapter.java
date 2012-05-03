package ness.cache2.guava;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import ness.cache2.NamespacedCache;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.TypeLiteral;
import com.nesscomputing.logging.Log;

/**
 * Provides a Guava Cache implementation backed by a namespaced NessCache.
 * @see GuavaCacheModuleBuilder
 */
class GuavaCacheAdapter<K, V> implements LoadingCache<K, V> {
    private static final Log LOG = Log.findLog();

    @SuppressWarnings("rawtypes")
    private static final CacheLoader NO_LOADER = new CacheLoader<Object, Object>() {
        @Override
        public Object load(Object key) throws Exception {
            throw new UnsupportedOperationException("This cache was not configured with a loader");
        }
    };

    private final NamespacedCache cache;
    private final TypeLiteral<K> kClass;
    private final Function<? super K, String> keySerializer;
    private final Function<? super V, byte[]> valueSerializer;
    private final Function<byte[], ? extends V> valueDeserializer;
    private final CacheLoader<? super K, ? extends V> loader;
    private final Duration expiry;
    private final Duration expiryJitter;

    private final AtomicBoolean bulkLoadFailed = new AtomicBoolean();


    @SuppressWarnings("unchecked")
    GuavaCacheAdapter(
            NamespacedCache cache,
            TypeLiteral<K> kClass,
            Function<? super K, String> keySerializer,
            Function<? super V, byte[]> valueSerializer,
            Function<byte[], ? extends V> valueDeserializer,
            CacheLoader<? super K, ? extends V> loader,
            Duration expiry,
            Duration expiryJitter)
    {
        this.cache = cache;
        this.kClass = kClass;
        this.keySerializer = ExceptionWrappingFunction.of(keySerializer);
        this.valueSerializer = ExceptionWrappingFunction.of(valueSerializer);
        this.valueDeserializer = ExceptionWrappingFunction.of(valueDeserializer);
        this.loader = Objects.firstNonNull(loader, NO_LOADER);
        this.expiry = expiry;
        this.expiryJitter = expiryJitter;
    }

    @Override
    public V getIfPresent(K key) {
        return getAllPresent(Collections.singleton(key)).get(key);
    }

    @Override
    public V get(final K key) throws ExecutionException {
        Preconditions.checkArgument(key != null, "null key");
        return get(key, new Callable<V>() {
            @Override
            public V call() throws Exception {
                return loader.load(key);
            }
        });
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
        Preconditions.checkArgument(key != null, "null key");
        V value = getIfPresent(key);
        if (value != null) {
            return value;
        }

        try {
            value = valueLoader.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }

        put(key, value);
        return value;
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<? extends K> keys) {
        Map<String, ? extends K> keyStrings = Maps.uniqueIndex(keys, keySerializer);
        Map<String, byte[]> response = cache.get(keyStrings.keySet());

        Builder<K, V> result = ImmutableMap.builder();

        for (Entry<String, byte[]> e : response.entrySet()) {
            K key = keyStrings.get(e.getKey());
            try
            {
                result.put(key, valueDeserializer.apply(e.getValue()));
            } catch (Exception exc)
            {
                invalidate(key);
                throw Throwables.propagate(exc);
            }
        }

        return result.build();
    }

    @SuppressWarnings("unchecked") // Safe because the resulting Map is immutable and we only widen the key / narrow the value
    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys) throws ExecutionException {
        ImmutableMap<K, V> partialResult = getAllPresent(keys);

        Set<? extends K> remaining = ImmutableSet.copyOf(Iterables.filter(keys, not(in(partialResult.keySet()))));

        if (remaining.isEmpty())
        {
            return partialResult;
        }

        Map<K, V> loaded = null;
        try {
            if (!bulkLoadFailed.get()) {
                loaded = (Map<K, V>) loader.loadAll(remaining);
            }
        } catch (UnsupportedOperationException e) {
            if (bulkLoadFailed.compareAndSet(false, true)) {
                LOG.warn(e, "Cache loader %s does not support bulk loads, disabling. Could get a nice performance benefit here!", loader);
            }
        } catch (Exception e) {
            LOG.error(e, "Exception from cache loader during getAll");
            return partialResult;
        }

        if (loaded == null) {
            loaded = Maps.newHashMap();
            for (K key : remaining) {
                try {
                    loaded.put(key, loader.load(key));
                } catch (Exception e) {
                    LOG.error(e, "Exception from cache loader during getAll");
                }
            }
        }


        for (Entry<K, V> e : loaded.entrySet())
        {
            put(e.getKey(), e.getValue());
        }

        if (!loaded.keySet().containsAll(remaining))
        {
            throw new IncompleteCacheLoadException(String.format(
                    "loader %s did not return keys %s for request of %s",
                    loader,
                    Sets.difference(remaining, loaded.keySet()),
                    remaining));
        }

        return ImmutableMap.<K, V>builder()
                .putAll(partialResult)
                .putAll(loaded)
                .build();
    }

    @Override
    public void refresh(K key) {
        Preconditions.checkArgument(key != null, "null key");
        try {
            put(key, loader.load(key));
        } catch (Exception e) {
            LOG.error(e, "Exception from cache loader during refresh");
        }
    }

    @Override
    public void put(K key, V value) {
        Preconditions.checkArgument(key != null, "null key");
        cache.set(keySerializer.apply(key), valueSerializer.apply(value), getExpiry());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void invalidate(Object key) {
        Preconditions.checkArgument(key != null, "null key");
        // Best we can do... if your key type is Foo<T> and you pass a Foo<V>, you're on your own :-/
        if (kClass.getRawType().isAssignableFrom(key.getClass())) {
            cache.clear(keySerializer.apply((K) key));
        }
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        for (Object key : keys) {
            Preconditions.checkArgument(key != null, "null key");
            invalidate(key);
        }
    }

    @Override
    public void invalidateAll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheStats stats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanUp() {
    }

    @Override
    public V getUnchecked(K key) {
        try {
            return get(key);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public V apply(K key) {
        return getUnchecked(key);
    }

    private DateTime getExpiry() {
        if (expiry == null) {
            return DateTime.now().plusYears(10);
        }

        DateTime result = DateTime.now().plus(expiry);

        if (expiryJitter != null) {
            result = result.plus((long) (expiryJitter.getMillis() * (Math.random() * 2.0 - 1.0)));
        }

        return result;
    }

    private static class ExceptionWrappingFunction<A, B> implements Function<A, B>
    {
        private final Function<A, B> func;

        ExceptionWrappingFunction(Function<A, B> func)
        {
            this.func = func;
        }

        static <A, B> ExceptionWrappingFunction<A, B> of(Function<A, B> func)
        {
            return new ExceptionWrappingFunction<A, B>(func);
        }

        @Override
        public B apply(A input)
        {
            try {
                return func.apply(input);
            } catch (Exception e)
            {
                throw new UncheckedExecutionException(e);
            }
        }
    }
}
