package ness.cache2;

import io.trumpet.log.Log;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;

import org.joda.time.DateTime;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 * Provide a cache based upon a memached server
 */
@Singleton
final class MemcacheProvider implements InternalCacheProvider {

    private static final Function<String, String> BASE64_ENCODER = new Function<String, String>() {
        @Override
        public String apply(final String input) {
            return new String(Base64.encode(input), Charsets.UTF_8);
        }
    };

    private static final Function<String, String> BASE64_DECODER = new Function<String, String>() {
        @Override
        public String apply(final String input) {
            return new String(Base64.decode(input), Charsets.UTF_8);
        }
    };

    private static final Callback<Boolean, byte[]> ADD_CALLBACK = new Callback<Boolean, byte[]>() {
        @Override
        public Future<Boolean> callback(final MemcachedClient client, final String key, final CacheStore<byte []> cacheStore) throws InterruptedException {
            return client.add(key,
                              computeMemcacheExpiry(cacheStore.getExpiry()),
                              cacheStore.getData());
        }
    };

    private static final Callback<Boolean, byte[]> SET_CALLBACK = new Callback<Boolean, byte[]>() {
        @Override
        public Future<Boolean> callback(final MemcachedClient client, final String key, final CacheStore<byte []> cacheStore) throws InterruptedException {
            return client.set(key,
                              computeMemcacheExpiry(cacheStore.getExpiry()),
                              cacheStore.getData());
        }
    };

    private static final Callback<Boolean, Void> CLEAR_CALLBACK = new Callback<Boolean, Void>() {
        @Override
        public Future<Boolean> callback(final MemcachedClient client, final String key, final CacheStore<Void> cacheStore) throws InterruptedException {
            return client.delete(key);
        }
    };


    private static final Log LOG = Log.findLog();
    private static final String SEPARATOR = "\u0001";

    private final MemcachedClientFactory clientFactory;
    private final CacheConfiguration config;
    private final Function<String,String> encoder;
    private final Function<String,String> decoder;

    @Inject
    MemcacheProvider(final CacheConfiguration config,
                     final MemcachedClientFactory clientFactory)
    {
        this.config = config;
        this.clientFactory = clientFactory;
        CacheConfiguration.EncodingType encodingType = config.getMemcachedEncoding();

        switch(encodingType) {
        case BASE64:
            this.encoder = BASE64_ENCODER;
            this.decoder = BASE64_DECODER;
            break;
        case NONE:
            this.encoder = Functions.identity();
            this.decoder = Functions.identity();
            break;
        default:
            throw new IllegalArgumentException("Unknown encoding type " + encodingType);
        }
    }

    @Override
    public void set(final String namespace, final Collection<CacheStore<byte []>> stores)
    {
        processOps(namespace, false, stores, SET_CALLBACK);
    }

    @Override
    public Map<String, Boolean> add(final String namespace, final Collection<CacheStore<byte []>> stores)
    {
        return processOps(namespace, true, stores, ADD_CALLBACK);
    }

    @Override
    public void clear(final String namespace, final Collection<String> keys)
    {
        processOps(namespace, false, CacheStores.forKeys(keys, null), CLEAR_CALLBACK);
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        MemcachedClient client = clientFactory.get();
        if (client == null) {
            return Collections.emptyMap();
        }

        Collection<String> preparedKeys = makeKeys(namespace, keys);
        final Map<String, Object> internalResult;
        try {
            // The assertion above protects this somewhat dodgy-looking cast.  Since the transcoder always
            // returns byte[], it is safe to cast the value Object to byte[].
            internalResult = client.getBulk(preparedKeys);

            final ImmutableMap.Builder<String, byte[]> transformedResults = ImmutableMap.builder();

            final String encodedNamespacePlusSeparator = encoder.apply(namespace) + SEPARATOR;
            final int prefixLength = encodedNamespacePlusSeparator.length();

            for (Entry<String, Object> e : internalResult.entrySet()) {
                if (e.getValue() == null) {
                    continue;
                }

                // This cast better works, otherwise, the memcached returned something besides a byte [] as value.
                transformedResults.put(decoder.apply(e.getKey().substring(prefixLength)), byte [].class.cast(e.getValue()));
            }

            return transformedResults.build();
        }
        catch (IllegalStateException ise) {
            LOG.errorDebug(ise, "Memcache Queue was full while loading keys!");
        }
        catch (OperationTimeoutException ote) {
            LOG.errorDebug(ote, "Operation timed out while loading keys!");
        }
        return Collections.emptyMap();
    }

    /** Memcache expects expiration dates in seconds since the epoch. */
    public static int computeMemcacheExpiry(@Nullable DateTime when)
    {
        return when == null ? -1 : Ints.saturatedCast(when.getMillis() / 1000);
    }

    private Collection<String> makeKeys(final String namespace, Collection<String> keys) {
        final String namespaceEncoded = encoder.apply(namespace);
        return Collections2.transform(keys, new Function<String, String>() {
            @Override
            public String apply(String key) {
                return makeKeyWithNamespaceAlreadyEncoded(namespaceEncoded, key);
            }
        });
    }

    private String makeKeyWithNamespaceAlreadyEncoded(String encodedNamespace, String key) {
        return encodedNamespace + SEPARATOR + encoder.apply(key);
    }

    private <F, D> Map<String, F> processOps(final String namespace, final boolean wait, final Collection<CacheStore<D>> stores, Callback<F, D> callback)
    {
        final MemcachedClient client = clientFactory.get();

        if (client == null) {
            return Collections.emptyMap();
        }

        final Map<String, Future<F>> futures = Maps.newHashMap();

        try {
            final String nsEncoded = encoder.apply(namespace);
            for (final CacheStore<D> cacheStore : stores) {

                final String key = cacheStore.getKey();

                Future<F> future = null;
                try {
                    future = callback.callback(client, makeKeyWithNamespaceAlreadyEncoded(nsEncoded, key), cacheStore);
                    futures.put(key, future);
                } catch (IllegalStateException ise) {
                    LOG.errorDebug(ise, "Memcache Queue was full while storing %s:%s", namespace, key);
                }

                syncCheck(future, namespace, cacheStore);
            }

            if (wait) {
                return waitFutures(namespace, futures);
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        return Collections.emptyMap();
    }

    private void syncCheck(final Future<?> future, final String namespace, final CacheStore<?> cacheStore)
        throws InterruptedException
    {
        if (future != null && config.isCacheSynchronous()) {
            try {
                future.get();
            } catch (ExecutionException e) {
                LOG.errorDebug(e.getCause(), "Cache entry %s:%s", namespace, cacheStore.getKey());
            }
        }
    }

    private <T> Map<String, T> waitFutures(final String namespace, final Map<String, Future<T>> futures)
        throws InterruptedException
    {
        final Map<String, T> results = Maps.newHashMap();
        for (Map.Entry<String, Future<T>> entry : futures.entrySet()) {
            try {
                results.put(entry.getKey(), entry.getValue().get());
            } catch (ExecutionException e) {
                LOG.errorDebug(e.getCause(), "Cache entry %s:%s", namespace, entry.getKey());
            }
        }
        return results;
    }

    public interface Callback<F, D>
    {
        Future<F> callback(MemcachedClient client, String nsKey, CacheStore<D> data) throws InterruptedException;
    }
}
