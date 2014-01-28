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

import java.net.InetSocketAddress;

import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap.EvictionPolicy;

import org.junit.After;
import org.junit.Before;
import org.kitei.testing.lessio.AllowNetworkListen;

/**
 * Set up a memcached on localhost:<ephemeral port>
 */
@AllowNetworkListen(ports = {0})
public abstract class BaseCacheIntegrationSetup extends BaseCachingTests
{
    protected final int PORT = NetUtils.findUnusedPort();
    private MemCacheDaemon<LocalCacheElement> daemon;

    @Before
    public final void setUpJMemcache()
        throws Exception
    {
        daemon = new MemCacheDaemon<LocalCacheElement>();

        CacheStorage<Key, LocalCacheElement> storage = ConcurrentLinkedHashMap.create(EvictionPolicy.FIFO, 10000, 10000000);

        daemon.setCache(new CacheImpl(storage));
        daemon.setBinary(true);
        daemon.setAddr(new InetSocketAddress("127.0.0.1", PORT));
        daemon.start();
    }

    @After
    public final void tearDownJMemcache()
    {
        daemon.stop();
        daemon = null;
    }
}
