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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap.EvictionPolicy;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.nesscomputing.config.Config;
import com.nesscomputing.lifecycle.Lifecycle;
import com.nesscomputing.lifecycle.LifecycleStage;
import com.nesscomputing.lifecycle.guice.LifecycleModule;
import com.nesscomputing.log4j.ConfigureStandaloneLogging;
import com.nesscomputing.logging.Log;
import com.nesscomputing.service.discovery.client.DiscoveryClient;
import com.nesscomputing.service.discovery.client.ReadOnlyDiscoveryClient;
import com.nesscomputing.service.discovery.client.ServiceInformation;
import com.nesscomputing.service.discovery.testing.client.MockedDiscoveryClient;
import com.nesscomputing.testing.lessio.AllowDNSResolution;
import com.nesscomputing.testing.lessio.AllowNetworkAccess;
import com.nesscomputing.testing.lessio.AllowNetworkListen;

/**
 * @author christopher
 *
 */
@AllowDNSResolution
@AllowNetworkListen(ports = {0})
@AllowNetworkAccess(endpoints = {"127.0.0.1:0"})
@Ignore // XXX: (hps) this test is too flaky to run and generally fails on linux.
public class NamedMemcacheTest {
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
        announce1 = ServiceInformation.forService("memcached", "1", "memcache", addr1.getHostName(), addr1.getPort());
        addr2 = new InetSocketAddress("127.0.0.1", NetUtils.findUnusedPort());
        daemon2 = createDaemon(addr2.getPort());
        announce2 = ServiceInformation.forService("memcached", "2", "memcache", addr2.getHostName(), addr2.getPort());
        addr3 = new InetSocketAddress("127.0.0.1", NetUtils.findUnusedPort());
        daemon3 = createDaemon(addr3.getPort());
        announce3 = ServiceInformation.forService("memcached", "3", "memcache", addr3.getHostName(), addr3.getPort());

        discovery.announce(announce1);
        discovery.announce(announce2);
        discovery.announce(announce3);

        final Config config = Config.getFixedConfig(
                                                    "ness.cache", "MEMCACHE",
                                                    "ness.cache.synchronous", "true",
                                                    "ness.cache.jmx", "false");

        Guice.createInjector(
                             new CacheModule("1"),
                             new CacheModule("2"),
                             new CacheModule("3"),
                             new LifecycleModule(),
                             new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();
                ConfigureStandaloneLogging.configure();

                bind (ReadOnlyDiscoveryClient.class).toInstance(discovery);
                requestInjection (NamedMemcacheTest.this);

                bind (Config.class).toInstance(config);
            }
        });
        lifecycle.executeTo(LifecycleStage.START_STAGE);
    }

    @After
    public final void tearDown() {
        lifecycle.executeTo(LifecycleStage.STOP_STAGE);
        daemon1.stop();
        daemon2.stop();
        daemon3.stop();
    }

    private final Set<String> allKeys = Sets.newHashSet();

    private void writeLots(NessCache cache) {
        Random r = new Random(RANDOM_SEED);
        NamespacedCache c = cache.withNamespace(NS);

        for (int i = 0; i < NUM_WRITES; i++) {
            byte[] data = new byte[4];
            r.nextBytes(data);
            String key = Integer.toString(r.nextInt());
            c.set(key, data, expiry);

            allKeys.add(key);
        }
    }

    private boolean verifyWrites(NessCache cache) {
        Random r = new Random(RANDOM_SEED);
        NamespacedCache c = cache.withNamespace(NS);

        for (int i = 0; i < NUM_WRITES; i++) {
            byte[] data = new byte[4];
            r.nextBytes(data);
            String key = Integer.toString(r.nextInt());
            if (!Arrays.equals(data, c.get(key))) {
                return false;
            }
        }

        return true;
    }

    private final DiscoveryClient discovery = MockedDiscoveryClient.builder().build();
    @Inject
    Lifecycle lifecycle;
    @Inject
    @Named("1")
    NessCache cache1;
    @Inject
    @Named("2")
    NessCache cache2;
    @Inject
    @Named("3")
    NessCache cache3;

    @Test
    public void testSimpleReadWrite() {
        LOG.info("Writing into cache 1...");
        writeLots(cache1);
        LOG.info("Verify cache 1...");
        assertTrue(verifyWrites(cache1));
        LOG.info("Writing into cache 2...");
        writeLots(cache2);
        LOG.info("Verify cache 2...");
        assertTrue(verifyWrites(cache2));
        LOG.info("Writing into cache 3...");
        writeLots(cache3);
        LOG.info("Verify cache 3...");
        assertTrue(verifyWrites(cache3));
    }

    @Test
    public void testLocalizedWrites() throws InterruptedException {
        LOG.info("Writing into cache 1...");
        //There appears to be some non-determinism based on startup time.  10ms appears to be a good threshold for this not to happen, so we do 100 so we won't see this again.
        Thread.sleep(100);
        writeLots(cache1);
        LOG.info("Verify cache 1...");
        assertTrue(verifyWrites(cache1));
        LOG.info("Verify cache 2...");
        assertFalse(verifyWrites(cache2));
        LOG.info("Verify cache 3...");
        assertFalse(verifyWrites(cache3));
    }
}

