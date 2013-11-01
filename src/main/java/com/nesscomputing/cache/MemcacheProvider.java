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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;

import org.joda.time.DateTime;

import com.nesscomputing.logging.Log;


/**
 * Provide a cache based upon a memached server
 */
@Singleton
final class MemcacheProvider implements InternalCacheProvider
{
    private final ConcurrentMap<String, NamespaceInfo> namespaceMap = Maps.newConcurrentMap();

    private static final Function<String, String> BASE64_ENCODER = new Function<String, String>() {
        @Override
        public String apply(final String input) {
            if (input == null) {
                return null;
            }
            return new String(Base64.encode(input), Charsets.UTF_8);
        }
    };

    private static final Function<String, String> BASE64_DECODER = new Function<String, String>() {
        @Override
        public String apply(final String input) {
            if (input == null) {
                return null;
            }
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

    private final MemcachedClientFactory clientFactory;
    private final CacheConfiguration config;
    private final Function<String,String> encoder;
    private final Function<String,String> decoder;
    private final String separator;

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

        this.separator = config.getMemcachedSeparator();
    }

    @Override
    public void set(final String namespace, final Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics cacheStatistics)
    {
        Collection<CacheStore<byte[]>> validStores = Collections2.filter(stores, validateWritePredicate);
        if (cacheStatistics != null) {
            cacheStatistics.incrementOversizedStores(stores.size() - validStores.size());
        }
        processOps(namespace, false, validStores, SET_CALLBACK);
    }

    @Override
    public Map<String, Boolean> add(final String namespace, final Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics cacheStatistics)
    {
        ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
        List<CacheStore<byte[]>> validStores = Lists.newArrayListWithExpectedSize(stores.size());
        for (CacheStore<byte[]> store : stores) {
            if (validateWrite(store)) {
                validStores.add(store);
            } else {
                builder.put(Maps.immutableEntry(store.getKey(), false));
            }
        }
        if (cacheStatistics != null) {
            cacheStatistics.incrementOversizedStores(stores.size() - validStores.size());
        }
        builder.putAll(processOps(namespace, true, validStores, ADD_CALLBACK));
        return builder.build();
    }

    @Override
    public void clear(final String namespace, final Collection<String> keys, @Nullable CacheStatistics cacheStatistics)
    {
        processOps(namespace, false, CacheStores.forKeys(keys, null), CLEAR_CALLBACK);
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys, @Nullable CacheStatistics cacheStatistics) {

        final MemcachedClient client = clientFactory.get();
        if (client == null) {
            return Collections.emptyMap();
        }

        final NamespaceInfo namespaceInfo = findNamespace(namespace);
        final int prefixLength = namespaceInfo.getPrefixLength();

        final Collection<String> preparedKeys = makeKeys(namespaceInfo, keys);
        final Map<String, Object> internalResult;
        try {
            // The assertion above protects this somewhat dodgy-looking cast.  Since the transcoder always
            // returns byte[], it is safe to cast the value Object to byte[].
            internalResult = client.getBulk(preparedKeys);

            final ImmutableMap.Builder<String, byte[]> transformedResults = ImmutableMap.builder();

            for (Entry<String, Object> e : internalResult.entrySet()) {
                if (e.getValue() == null) {
                    continue;
                }

                // This cast better works, otherwise, the memcached returned something besides a byte [] as value.
                transformedResults.put(decoder.apply(e.getKey().substring(prefixLength)), byte [].class.cast(e.getValue()));
            }

            return transformedResults.build();
        }
        catch (OperationTimeoutException ote) {
            LOG.errorDebug(ote, "Operation timed out while loading keys for cache %s, namespace %s!", clientFactory.getCacheName(), namespace);
        }
        catch (CancellationException ce) {
            LOG.errorDebug(ce, "Operation cancelled while loading keys for cache %s, namespace %s!", clientFactory.getCacheName(), namespace);
        }
        catch (IllegalStateException ise) {
            LOG.errorDebug(ise, "Memcache Queue was full while loading keys for cache %s, namespace %s!", clientFactory.getCacheName(), namespace);
        }
        return Collections.emptyMap();
    }

    /** Memcache expects expiration dates in seconds since the epoch. */
    public static int computeMemcacheExpiry(@Nullable DateTime when)
    {
        return when == null ? -1 : Ints.saturatedCast(when.getMillis() / 1000);
    }

    private Collection<String> makeKeys(final NamespaceInfo namespaceInfo, Collection<String> keys) {
        final String encodedNamespace = namespaceInfo.getEncodedNamespace();
        return Collections2.transform(keys, new Function<String, String>() {
            @Override
            public String apply(final String key) {
                return encodedNamespace + encoder.apply(key);
            }
        });
    }

    private long lastWarnAboutNullClient = 0;
    private <F, D> Map<String, F> processOps(final String namespace, final boolean wait, final Collection<CacheStore<D>> stores, Callback<F, D> callback)
    {
        final MemcachedClient client = clientFactory.get();
        if (client == null) {
            long now = System.currentTimeMillis();
            if(now - lastWarnAboutNullClient > 60_000 * 60){ // Every 60 minutes
                lastWarnAboutNullClient = now;
                LOG.error("Failed to access to memcache because clientFactory didn't have any clients");
            }
            return Collections.emptyMap();
        }

        final NamespaceInfo namespaceInfo = findNamespace(namespace);
        final String encodedNamespace = namespaceInfo.getEncodedNamespace();
        final Map<String, Future<F>> futures = Maps.newHashMap();

        try {
            for (final CacheStore<D> cacheStore : stores) {

                final String key = cacheStore.getKey();

                Future<F> future = null;
                try {
                    future = callback.callback(client, encodedNamespace + encoder.apply(key), cacheStore);
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
                if (!future.isCancelled()) {
                    future.get();
                }
            }
            catch (ExecutionException e) {
                LOG.errorDebug(e.getCause(), "Cache entry %s:%s", namespace, cacheStore.getKey());
            }
            catch (CancellationException ce) {
                LOG.trace("Cache entry %s:%s was cancelled", namespace, cacheStore.getKey());
            }
        }
    }

    private <T> Map<String, T> waitFutures(final String namespace, final Map<String, Future<T>> futures)
        throws InterruptedException
    {
        final Map<String, T> results = Maps.newHashMap();
        for (Map.Entry<String, Future<T>> entry : futures.entrySet()) {
            try {
                final Future<T> value = entry.getValue();
                if (!value.isCancelled()) {
                    results.put(entry.getKey(), value.get());
                }
            } catch (ExecutionException e) {
                LOG.errorDebug(e.getCause(), "Cache entry %s:%s", namespace, entry.getKey());
            }
            catch (CancellationException ce) {
                LOG.trace("Cache entry %s:%s was cancelled", namespace, entry.getKey());
            }

        }
        return results;
    }

    private NamespaceInfo findNamespace(final String namespace)
    {
        NamespaceInfo namespaceInfo = namespaceMap.get(namespace);
        if (namespaceInfo == null) {
            final NamespaceInfo newNamespaceInfo = new NamespaceInfo(namespace);
            namespaceInfo = namespaceMap.putIfAbsent(namespace, newNamespaceInfo);
            if (namespaceInfo == null) {
                namespaceInfo = newNamespaceInfo;
            }
        }
        return namespaceInfo;
    }

    private final Predicate<CacheStore<byte[]>> validateWritePredicate = new Predicate<CacheStore<byte[]>>() {
        @Override
        public boolean apply(@Nullable CacheStore<byte[]> input) {
            return validateWrite(input);
        }
    };

    private boolean validateWrite(CacheStore<byte[]> input) {
        int maxValueSize = config.getMemcachedMaxValueSize();
        byte[] data = input.getData();
        if (maxValueSize > 0 && data != null && data.length > maxValueSize) {
            LOG.debug("Rejecting write of %s because length %s exceeds maximum %s", input.getKey(), data.length, maxValueSize);
            return false;
        }
        return true;
    }

    public interface Callback<F, D>
    {
        Future<F> callback(MemcachedClient client, String nsKey, CacheStore<D> data) throws InterruptedException;
    }

    private class NamespaceInfo
    {
        private final String encodedNamespace;
        private final int prefixLength;

        private NamespaceInfo(final String namespace)
        {
            this.encodedNamespace = encoder.apply(namespace) + separator;
            this.prefixLength = encodedNamespace.length();
        }

        private String getEncodedNamespace()
        {
            return encodedNamespace;
        }

        private int getPrefixLength()
        {
            return prefixLength;
        }
    }
}
