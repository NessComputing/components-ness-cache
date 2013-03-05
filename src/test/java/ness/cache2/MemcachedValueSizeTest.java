package ness.cache2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.Collections;

import com.google.common.collect.Lists;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap.EvictionPolicy;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nesscomputing.testing.lessio.AllowDNSResolution;
import com.nesscomputing.testing.lessio.AllowNetworkAccess;
import com.nesscomputing.testing.lessio.AllowNetworkListen;

@AllowDNSResolution
@AllowNetworkListen(ports = {11212})
@AllowNetworkAccess(endpoints = {"127.0.0.1:11212"})
public class MemcachedValueSizeTest
{
    private MemCacheDaemon<LocalCacheElement> daemon;
    private MemcachedClient client;
    private InetSocketAddress addr;
    private MemcacheProvider provider;

    @Before
    public void setUp() throws Exception {
        addr = new InetSocketAddress("127.0.0.1", 11212);

        daemon = new MemCacheDaemon<LocalCacheElement>();

        CacheStorage<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap.create(EvictionPolicy.FIFO, 10000, 10000000);

        daemon.setCache(new CacheImpl(storage));
        daemon.setBinary(false);
        daemon.setAddr(new InetSocketAddress("127.0.0.1", addr.getPort()));
        daemon.start();

        client = new MemcachedClient(new DefaultConnectionFactory() {
            @Override
            public FailureMode getFailureMode() {
                return FailureMode.Retry;
            }
        }, Lists.newArrayList(addr));

        MemcachedClientFactory clientFactory = EasyMock.createMock(MemcachedClientFactory.class);
        EasyMock.expect(clientFactory.get()).andReturn(client).anyTimes();
        EasyMock.replay(clientFactory);
        provider = new MemcacheProvider(new CacheConfiguration() { }, clientFactory);
    }

    @Test
    public void testMaxSize() throws Exception
    {
        CacheStatistics stats = new CacheStatistics("test");
        assertEquals(0, stats.getOversizedStores());
        int size = new CacheConfiguration() { }.getMemcachedMaxValueSize() + 1;
        provider.set("a", Collections.singleton(new CacheStore<byte[]>("a", new byte[size], DateTime.now().plusMinutes(1))), stats);
        assertTrue(provider.get("a", Collections.singleton("a"), null).isEmpty());
        assertEquals(1, stats.getOversizedStores());
    }

    @After
    public final void tearDown() {
        client.shutdown();
        daemon.stop();
    }
}
