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
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.ConfigurationFactory;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.joda.time.DateTime;

import com.nesscomputing.lifecycle.Lifecycle;
import com.nesscomputing.lifecycle.LifecycleListener;
import com.nesscomputing.lifecycle.LifecycleStage;

/**
 * An in-JVM cache, currently backed by EHCache
 */
@Singleton
public class JvmCacheProvider implements InternalCacheProvider {

    private final Cache ehCache;

    @Inject
    JvmCacheProvider(Lifecycle lifecycle) {
        ehCache = new Cache(new CacheConfiguration("ness.cache." + hashCode(), 100000)
                .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU)
                .persistence(new PersistenceConfiguration().strategy(Strategy.NONE)));

        final Configuration ehcacheConfig = ConfigurationFactory.parseConfiguration();
        ehcacheConfig.setUpdateCheck(false);
        final CacheManager cacheManager = CacheManager.create(ehcacheConfig);

        cacheManager.addCache(ehCache);

        lifecycle.addListener(LifecycleStage.STOP_STAGE, new LifecycleListener() {
            @Override
            public void onStage(LifecycleStage lifecycleStage) {
                cacheManager.shutdown();
            }
        });
    }

    @Override
    public void set(String namespace, Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics cacheStatistics) {
        for (CacheStore<byte []> e : stores) {
            ehCache.put(new Element(
                    makeKey(namespace, e.getKey()),
                    e));
        }
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys, @Nullable CacheStatistics cacheStatistics) {
        Map<String, byte[]> map = Maps.newHashMap();
        for (String key : keys) {
            Element value = ehCache.get(makeKey(namespace, key));

            if (value != null && value.getObjectValue() != null) {
                @SuppressWarnings("unchecked")
                CacheStore<byte []> storedEntry = (CacheStore<byte []>)value.getObjectValue();
                final DateTime expiry = storedEntry.getExpiry();
                final byte [] data = storedEntry.getData();
                if ((expiry == null || expiry.isAfterNow()) && data != null) {
                    map.put(key, data);
                } else {
                    clear(namespace, Collections.singleton(key), cacheStatistics);
                }
            }

        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public void clear(String namespace, Collection<String> keys, @Nullable CacheStatistics cacheStatistics) {
        for (String key : keys) {
            ehCache.remove(makeKey(namespace, key));
        }
    }

    @Override
    public Map<String, Boolean> add(final String namespace, final Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics cacheStatistics)
    {
        final Map<String, Boolean> resultMap = Maps.newHashMap();
        for (CacheStore<byte []> e : stores) {
            final Element old = ehCache.putIfAbsent(new Element(makeKey(namespace, e.getKey()), e));
            resultMap.put(e.getKey(), old == null);
        }
        return resultMap;
    }

    private Entry<String, String> makeKey(String namespace, String key) {
        return Maps.immutableEntry(namespace, key);
    }
}
