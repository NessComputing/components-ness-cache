package ness.cache2;

import static org.junit.Assert.assertArrayEquals;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import ness.discovery.client.DiscoveryClient;
import ness.discovery.client.ReadOnlyDiscoveryClient;
import ness.discovery.client.ServiceInformation;
import ness.discovery.client.testing.MockedDiscoveryClient;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;
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
@AllowNetworkListen(ports = {11212, 11213, 11214})
@AllowNetworkAccess(endpoints = {"127.0.0.1:11212", "127.0.0.1:11213", "127.0.0.1:11214"})
public class RemoveReAddNodeTest {
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

        final Config config = Config.getFixedConfig(ImmutableMap.of("ness.cache", "MEMCACHE",
                                                                                "ness.cache.synchronous", "true",
                                                                                "ness.cache.jmx", "false"));
        final Injector injector = Guice.createInjector(new ConfigModule(config),
                                                       new CacheModule(config, null, true),
                                                       new LifecycleModule(),
                                                       new AbstractModule() {
                                                           @Override
                                                           protected void configure() {
                                                               bind (ReadOnlyDiscoveryClient.class).toInstance(discovery);
                                                           }
                                                       });
        injector.injectMembers(this);

        Assert.assertNotNull(lifecycle);
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
    private Lifecycle lifecycle = null;

    @Inject
    private MemcachedClientFactory clientFactory = null;

    @Inject
    private Cache cache = null;

    @Test
    public void testAddingCache() throws Exception {
        writeLots();
        verifyWrites();

        long items1 = daemon1.getCache().getCurrentItems();
        long items2 = daemon2.getCache().getCurrentItems();
        long items3 = daemon3.getCache().getCurrentItems();
        LOG.info("Cache distribution: %d %d %d", items1, items2, items3);

        checkCaches(items1, items2, items3);
        Assert.assertEquals(items1 + items2 + items3, cache.get(NS, allKeys.keySet()).size());

        int generation = clientFactory.getTopologyGeneration();
        discovery.unannounce(announce2);
        clientFactory.waitTopologyChange(generation);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Assert.assertEquals(items1 + items3, cache.get(NS, allKeys.keySet()).size());

        generation = clientFactory.getTopologyGeneration();
        discovery.announce(announce2);
        clientFactory.waitTopologyChange(generation);
        Assert.assertEquals(generation + 1, clientFactory.getTopologyGeneration());

        checkCaches(items1, items2, items3);
        Assert.assertEquals(items1 + items2 + items3, cache.get(NS, allKeys.keySet()).size());

        verifyWrites();
    }
}
