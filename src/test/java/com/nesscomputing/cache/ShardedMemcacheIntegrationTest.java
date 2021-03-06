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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.nesscomputing.service.discovery.client.DiscoveryClient;
import com.nesscomputing.service.discovery.client.ReadOnlyDiscoveryClient;
import com.nesscomputing.service.discovery.client.ServiceInformation;
import com.nesscomputing.service.discovery.testing.client.MockedDiscoveryClient;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.nesscomputing.config.Config;
import com.nesscomputing.lifecycle.Lifecycle;
import com.nesscomputing.lifecycle.LifecycleStage;
import com.nesscomputing.lifecycle.guice.LifecycleModule;
import com.nesscomputing.logging.Log;
import com.nesscomputing.testing.lessio.AllowDNSResolution;
import com.nesscomputing.testing.lessio.AllowNetworkAccess;
import com.nesscomputing.testing.lessio.AllowNetworkListen;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap.EvictionPolicy;

@AllowDNSResolution
@AllowNetworkListen(ports = {0})
@AllowNetworkAccess(endpoints = {"127.0.0.1:0"})
@Ignore // XXX: (scs) this test is too flaky to run.  This should really be fixed but I don't have the time at the moment :-(
public class ShardedMemcacheIntegrationTest {
    private static final Log LOG = Log.findLog();
    private static final long RANDOM_SEED = 1234;
    private static final int NUM_WRITES = 1000;
    private static final String NS = "shard-integration-test";

    private MemCacheDaemon<LocalCacheElement> daemon1, daemon2, daemon3;
    private ServiceInformation announce1, announce2, announce3;
    private InetSocketAddress addr1, addr2, addr3;

    private final DateTime expiry = new DateTime().plusYears(100);

    public final MemCacheDaemon<LocalCacheElement> createDaemon(int port) {
        MemCacheDaemon<LocalCacheElement> daemon = new MemCacheDaemon<LocalCacheElement>();

        CacheStorage<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap.create(EvictionPolicy.FIFO, 10000, 10000000);

        daemon.setCache(new CacheImpl(storage));
        daemon.setBinary(true);
        daemon.setAddr(new InetSocketAddress("127.0.0.1", port));
        daemon.start();

        return daemon;
    }


    final CacheConfiguration configuration = new CacheConfiguration() {
        @Override
        public CacheType getCacheType() {
            return CacheType.MEMCACHE;
        }
        @Override
        public boolean isCacheSynchronous() {
            return true;
        }
        @Override
        public boolean isJmxEnabled() {
            return false;
        }
    };

    @Before
    public final void setUp() {
        addr1 = new InetSocketAddress("127.0.0.1", NetUtils.findUnusedPort());
        daemon1 = createDaemon(addr1.getPort());
        announce1 = ServiceInformation.forService("memcached", "test", "memcache", addr1.getHostName(), addr1.getPort());
        addr2 = new InetSocketAddress("127.0.0.1",  NetUtils.findUnusedPort());
        daemon2 = createDaemon(addr2.getPort());
        announce2 = ServiceInformation.forService("memcached", "test", "memcache", addr2.getHostName(), addr2.getPort());
        addr3 = new InetSocketAddress("127.0.0.1",  NetUtils.findUnusedPort());
        daemon3 = createDaemon(addr3.getPort());
        announce3 = ServiceInformation.forService("memcached", "test", "memcache", addr3.getHostName(), addr3.getPort());

        discovery.announce(announce1);
        discovery.announce(announce2);
        discovery.announce(announce3);

        final Config config = Config.getFixedConfig("ness.cache", "MEMCACHE",
                                                    "ness.cache.synchronous", "true",
                                                    "ness.cache.jmx", "false");

        CacheModule cacheModule = new CacheModule("test");
        Guice.createInjector(cacheModule,
                             new LifecycleModule(),
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (ShardedMemcacheIntegrationTest.this);
                bind (ReadOnlyDiscoveryClient.class).toInstance(discovery);

                bind (Config.class).toInstance(config);
            }
        });

        cacheTopology = cacheModule.getChildInjector().getInstance(CacheTopologyProvider.class);
        clientFactory = cacheModule.getChildInjector().getInstance(MemcachedClientFactory.class);

        lifecycle.executeTo(LifecycleStage.START_STAGE);
    }

    @After
    public final void tearDown() {
        lifecycle.executeTo(LifecycleStage.STOP_STAGE);
        daemon1.stop();
        daemon2.stop();
        daemon3.stop();
    }

    private final Map<String, byte[]> allKeys = Maps.newHashMap();

    private void writeLots() {
        Random r = new Random(RANDOM_SEED);
        NamespacedCache c = cache.withNamespace(NS);

        for (int i = 0; i < NUM_WRITES; i++) {
            byte[] data = new byte[4];
            r.nextBytes(data);
            String key = UUID.randomUUID().toString();
            c.set(key, data, expiry);

            allKeys.put(key, data);
        }

        Assert.assertEquals(NUM_WRITES, allKeys.size());
    }

    private void verifyWrites() {
        NamespacedCache c = cache.withNamespace(NS);

        for (Map.Entry<String, byte[]> key : allKeys.entrySet()) {
            assertArrayEquals("verify failed at key " + key.getKey(), key.getValue(), c.get(key.getKey()));
        }
    }

    private void checkCaches(final long items1, final long items2, final long items3)
    {
        Assert.assertEquals(items1, daemon1.getCache().getCurrentItems());
        Assert.assertEquals(items2, daemon2.getCache().getCurrentItems());
        Assert.assertEquals(items3, daemon3.getCache().getCurrentItems());
    }

    private final DiscoveryClient discovery = MockedDiscoveryClient.builder().build();
    @Inject
    Lifecycle lifecycle;

    CacheTopologyProvider cacheTopology;
    MemcachedClientFactory clientFactory;

    @Inject
    @Named("test")
    NessCache cache;

    @Test
    public void testSimpleClusterReconfiguration() {
        assertEquals(ImmutableList.of(addr1, addr2, addr3), cacheTopology.get());
        discovery.unannounce(announce2);
        assertEquals(ImmutableList.of(addr1, addr3), cacheTopology.get());
        discovery.announce(announce2);
        assertEquals(ImmutableList.of(addr1, addr2, addr3), cacheTopology.get());
    }

    @Test
    public void testDistributesWrites() {
        writeLots();
        verifyWrites();
        long items1 = daemon1.getCache().getCurrentItems();
        long items2 = daemon2.getCache().getCurrentItems();
        long items3 = daemon3.getCache().getCurrentItems();
        LOG.info("Cache distribution: %d %d %d", items1, items2, items3);
        assertTrue("" + items1, items1 > 250 && items1 < 400);
        assertTrue("" + items2, items2 > 250 && items2 < 400);
        assertTrue("" + items3, items3 > 250 && items3 < 400);
    }

    @Test
    public void testUnannouncedCaches() throws Exception {
        writeLots();
        verifyWrites();

        long items1 = daemon1.getCache().getCurrentItems();
        long items2 = daemon2.getCache().getCurrentItems();
        long items3 = daemon3.getCache().getCurrentItems();
        LOG.info("Cache distribution: %d %d %d", items1, items2, items3);

        checkCaches(items1, items2, items3);
        Thread.sleep(10);
        Assert.assertEquals(items1 + items2 + items3, cache.get(NS, allKeys.keySet()).size());

        int generation = clientFactory.getTopologyGeneration();
        discovery.unannounce(announce2);
        clientFactory.waitTopologyChange(generation);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Thread.sleep(10);
        Assert.assertEquals(items1 + items3, cache.get(NS, allKeys.keySet()).size());

        generation = clientFactory.getTopologyGeneration();
        discovery.unannounce(announce1);
        clientFactory.waitTopologyChange(generation);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Thread.sleep(10);
        Assert.assertEquals(items3, cache.get(NS, allKeys.keySet()).size());

        generation = clientFactory.getTopologyGeneration();
        discovery.unannounce(announce3);
        clientFactory.waitTopologyChange(generation);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Thread.sleep(10);
        Assert.assertEquals(0, cache.get(NS, allKeys.keySet()).size());

        generation = clientFactory.getTopologyGeneration();
        discovery.announce(announce2);
        clientFactory.waitTopologyChange(generation);
        Thread.sleep(10);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Thread.sleep(10);
        Assert.assertEquals(items2, cache.get(NS, allKeys.keySet()).size());

        generation = clientFactory.getTopologyGeneration();
        discovery.announce(announce3);
        clientFactory.waitTopologyChange(generation);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Thread.sleep(10);
        Assert.assertEquals(items2 + items3, cache.get(NS, allKeys.keySet()).size());

        generation = clientFactory.getTopologyGeneration();
        discovery.announce(announce1);
        clientFactory.waitTopologyChange(generation);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Thread.sleep(10);
        Assert.assertEquals(items1 + items2 + items3, cache.get(NS, allKeys.keySet()).size());

        verifyWrites();
    }

    @Test
    @Ignore // Pending http://code.google.com/p/spymemcached/issues/detail?id=189
    public void testCrashedCaches() throws Exception {
        writeLots();

        long items1 = daemon1.getCache().getCurrentItems();
        long items2 = daemon2.getCache().getCurrentItems();
        long items3 = daemon3.getCache().getCurrentItems();
        LOG.info("Cache distribution: %d %d %d", items1, items2, items3);

        Assert.assertEquals(items1 + items2 + items3, cache.get(NS, allKeys.keySet()).size());

        daemon2.stop();

        cache.get(NS, allKeys.keySet());

        Assert.assertEquals(items1 + items3, cache.get(NS, allKeys.keySet()).size());

        daemon1.stop();
        Assert.assertEquals(items1, cache.get(NS, allKeys.keySet()).size());

        daemon3.stop();
        assertEquals(0, cache.get(NS, allKeys.keySet()).size());
    }
}
