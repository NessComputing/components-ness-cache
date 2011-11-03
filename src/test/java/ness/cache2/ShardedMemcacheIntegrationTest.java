package ness.cache2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;
import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.LifecycleModule;
import io.trumpet.log.Log;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;

import ness.cache2.Cache;
import ness.cache2.CacheConfiguration;
import ness.cache2.CacheModule;
import ness.cache2.CacheTopologyProvider;
import ness.cache2.MemcachedClientFactory;
import ness.cache2.NamespacedCache;
import ness.discovery.client.DiscoveryClient;
import ness.discovery.client.ReadOnlyDiscoveryClient;
import ness.discovery.client.ServiceInformation;
import ness.discovery.client.testing.MockedDiscoveryClient;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.kaching.platform.testing.AllowDNSResolution;
import com.kaching.platform.testing.AllowNetworkAccess;
import com.kaching.platform.testing.AllowNetworkListen;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap.EvictionPolicy;

@AllowDNSResolution
@AllowNetworkListen(ports = {11212, 11213, 11214})
@AllowNetworkAccess(endpoints = {"127.0.0.1:11212", "127.0.0.1:11213", "127.0.0.1:11214"})
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
        addr1 = new InetSocketAddress("127.0.0.1", 11212);
        daemon1 = createDaemon(addr1.getPort());
        announce1 = ServiceInformation.forService("memcached", null, "memcache", addr1.getHostName(), addr1.getPort());
        addr2 = new InetSocketAddress("127.0.0.1", 11213);
        daemon2 = createDaemon(addr2.getPort());
        announce2 = ServiceInformation.forService("memcached", null, "memcache", addr2.getHostName(), addr2.getPort());
        addr3 = new InetSocketAddress("127.0.0.1", 11214);
        daemon3 = createDaemon(addr3.getPort());
        announce3 = ServiceInformation.forService("memcached", null, "memcache", addr3.getHostName(), addr3.getPort());

        discovery.announce(announce1);
        discovery.announce(announce2);
        discovery.announce(announce3);

        final TestingConfigModule tcm = new TestingConfigModule();
        final Config config = tcm.getConfig();

        final Module testModule = Modules.override(new CacheModule(config, null, true)).with(new AbstractModule() {
            @Override
            public void configure()
            {
                bind(CacheConfiguration.class).toInstance(configuration);
            }
        });

        Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (ShardedMemcacheIntegrationTest.this);
                install (tcm);
                install (testModule);
                install (new LifecycleModule());

                bind (ReadOnlyDiscoveryClient.class).toInstance(discovery);
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

    private void writeLots() {
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

    private void verifyWrites() {
        Random r = new Random(RANDOM_SEED);
        NamespacedCache c = cache.withNamespace(NS);

        for (int i = 0; i < NUM_WRITES; i++) {
            byte[] data = new byte[4];
            r.nextBytes(data);
            String key = Integer.toString(r.nextInt());
            assertArrayEquals("verify failed at key " + key, data, c.get(key));
        }
    }

    private final DiscoveryClient discovery = MockedDiscoveryClient.builder().build();
    @Inject
    Lifecycle lifecycle;
    @Inject
    CacheTopologyProvider cacheTopology;
    @Inject
    MemcachedClientFactory clientFactory;
    @Inject
    Cache cache;

    @Test
    public void testSimpleClusterReconfiguration() {
        assertEquals(ImmutableSet.of(addr1, addr2, addr3), cacheTopology.get());
        discovery.unannounce(announce2);
        assertEquals(ImmutableSet.of(addr1, addr3), cacheTopology.get());
        discovery.announce(announce2);
        assertEquals(ImmutableSet.of(addr1, addr2, addr3), cacheTopology.get());
    }

    @Test
    public void testDistributesWrites() {
        writeLots();
        verifyWrites();
        long items1 = daemon1.getCache().getCurrentItems();
        long items2 = daemon2.getCache().getCurrentItems();
        long items3 = daemon3.getCache().getCurrentItems();
        LOG.info("%d %d %d", items1, items2, items3);
        assertTrue("" + items1, items1 > 250 && items1 < 400);
        assertTrue("" + items2, items2 > 250 && items2 < 400);
        assertTrue("" + items3, items3 > 250 && items3 < 400);
    }

    @Test
    public void testUnannouncedCaches() throws Exception {
        writeLots();

        int size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.9 * NUM_WRITES && size <= 1.0 * NUM_WRITES);

        clientFactory.readyAwaitTopologyChange();
        discovery.unannounce(announce2);
        clientFactory.awaitTopologyChange();

        size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.6 * NUM_WRITES && size <= 0.7 * NUM_WRITES);

        clientFactory.readyAwaitTopologyChange();
        discovery.unannounce(announce1);
        clientFactory.awaitTopologyChange();

        size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.3 * NUM_WRITES && size <= 0.4 * NUM_WRITES);

        clientFactory.readyAwaitTopologyChange();
        discovery.unannounce(announce3);
        clientFactory.awaitTopologyChange();

        size = cache.get(NS, allKeys).size();
        assertEquals("" + size, 0, size);

        clientFactory.readyAwaitTopologyChange();
        discovery.announce(announce2);
        clientFactory.awaitTopologyChange();

        size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.3 * NUM_WRITES && size <= 0.4 * NUM_WRITES);

        clientFactory.readyAwaitTopologyChange();
        discovery.announce(announce3);
        clientFactory.awaitTopologyChange();

        size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.6 * NUM_WRITES && size <= 0.7 * NUM_WRITES);

        clientFactory.readyAwaitTopologyChange();
        discovery.announce(announce1);
        clientFactory.awaitTopologyChange();

        size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.9 * NUM_WRITES && size <= 1.0 * NUM_WRITES);

        verifyWrites();
    }

    @Test
    @Ignore // Pending http://code.google.com/p/spymemcached/issues/detail?id=189
    public void testCrashedCaches() throws Exception {
        writeLots();

        int size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.9 * NUM_WRITES && size <= 1.0 * NUM_WRITES);

        daemon2.stop();

        cache.get(NS, allKeys);

        size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.6 * NUM_WRITES && size <= 0.7 * NUM_WRITES);

        daemon1.stop();

        size = cache.get(NS, allKeys).size();
        assertTrue("" + size, size >= 0.3 * NUM_WRITES && size <= 0.4 * NUM_WRITES);

        daemon3.stop();

        size = cache.get(NS, allKeys).size();
        assertEquals("" + size, 0, size);
    }
}
