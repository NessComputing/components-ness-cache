package ness.cache2;

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;

import com.nesscomputing.testing.lessio.AllowNetworkListen;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap.EvictionPolicy;

/**
 * Set up a memcached on localhost:11212
 */
@AllowNetworkListen(ports = {11212})
public abstract class BaseCacheIntegrationSetup extends BaseCachingTests {

    private MemCacheDaemon<LocalCacheElement> daemon;

    @Before
    public final void setUpJMemcache() {
        daemon = new MemCacheDaemon<LocalCacheElement>();

        CacheStorage<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap.create(EvictionPolicy.FIFO, 10000, 10000000);

        daemon.setCache(new CacheImpl(storage));
        daemon.setBinary(true);
        daemon.setAddr(new InetSocketAddress("127.0.0.1", 11212));
        daemon.start();
    }

    @After
    public final void tearDownJMemcache() {
        daemon.stop();
        daemon = null;
    }
}
