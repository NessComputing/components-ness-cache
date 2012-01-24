package ness.cache2;

import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.OnStage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.likeness.logging.Log;

/**
 * Maintain a {@link MemcachedClient} which is always connected to the currently operating
 * memcached cluster.  Periodically uses the {@link CacheTopologyProvider} and if there is a change,
 * recreate the client.
 */
@Singleton
@ThreadSafe
class MemcachedClientFactory {
    private static final Log LOG = Log.findLog();

    private final AtomicReference<MemcachedClient> client = new AtomicReference<MemcachedClient>();
    private final AtomicReference<ScheduledExecutorService> clientReconfigurationService = new AtomicReference<ScheduledExecutorService>();
    private final AtomicInteger topologyGeneration = new AtomicInteger();

    private final AtomicReference<ImmutableList<InetSocketAddress>> addrHolder = new AtomicReference<ImmutableList<InetSocketAddress>>(ImmutableList.<InetSocketAddress>of());

    private final CacheTopologyProvider cacheTopology;

    private final NessMemcachedConnectionFactory connectionFactory;
    private final String cacheName;
    private final CacheConfiguration configuration;

    @Inject
    MemcachedClientFactory(final CacheConfiguration configuration,
                           final CacheTopologyProvider cacheTopology,
                           final NessMemcachedConnectionFactory connectionFactory,
                           @Nullable @Named("cacheName") final String cacheName)
    {

        this.cacheTopology = cacheTopology;
        this.cacheName = Objects.firstNonNull(cacheName, "[default]");
        this.configuration = configuration;

        this.connectionFactory = connectionFactory;
    }

    @OnStage(LifecycleStage.START)
    public void start() throws IOException
    {
        Preconditions.checkState(clientReconfigurationService.get() == null, "client is already started!");

        final ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("memcached-discovery-" + cacheName).setDaemon(true).build();
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, tf);
        if(clientReconfigurationService.compareAndSet(null, executor)) {
            LOG.info("Kicking off memcache topology discovery thread");
            final MemcachedDiscoveryUpdate updater = new MemcachedDiscoveryUpdate();

            // Bring up the client with the initial topology.
            final ImmutableList<InetSocketAddress> cacheAddrs = cacheTopology.get();
            if (!cacheAddrs.isEmpty()) {
                final MemcachedClient memcachedClient = new MemcachedClient(connectionFactory, cacheAddrs);
                client.set(memcachedClient);
                // addrHolder.set(cacheAddrs);
                LOG.info("Starting memcached client for cache %s on %s", cacheName, cacheAddrs);
            }
            else {
                LOG.warn("No caches found for cache %s, delaying startup!", cacheName);
            }

            final long rediscoveryInterval = configuration.getCacheServerRediscoveryInterval().getMillis();
            executor.scheduleAtFixedRate(updater, rediscoveryInterval, rediscoveryInterval, TimeUnit.MILLISECONDS);
        }
        else {
            LOG.warn("Race condition while starting discovery thread!");
            executor.shutdown();
        }
    }

    @OnStage(LifecycleStage.STOP)
    public void stop()
    {
        final ScheduledExecutorService executor = clientReconfigurationService.getAndSet(null);
        if (executor != null) {
            executor.shutdown();

            //  Since the executor service is shutdown, no more updates may happen.  So this client
            // is the final one that will be created, and client will not be concurrently modified
            final MemcachedClient clientToShutdown = client.getAndSet(null);

            if (clientToShutdown != null) {
                clientToShutdown.shutdown(30, TimeUnit.SECONDS); // Shut down gracefully
            }

            LOG.info("Caching system stopped");
        }
        else {
            LOG.info("Caching system was already stopped!");
        }
    }

    // This method must be very cheap, it is called per-operation
    public MemcachedClient get()
    {
        return client.get();
    }

    private class MemcachedDiscoveryUpdate implements Runnable
    {
        @Override
        public void run()
        {
            // Guaranteed to not run multiply so no additional coordination is required; see ScheduledExecutorService.
            LOG.trace("Cache prior to discovery: %s", client.get());

            // Discover potentially new cluster topology
            final ImmutableList<InetSocketAddress> newAddrs = cacheTopology.get();
            final ImmutableList<InetSocketAddress> addrs = addrHolder.get();

            if (addrs != null && addrs.equals(newAddrs)) {
                LOG.trace("Topology change ignored, identical list of servers (%s)", addrs);
            }
            else {
                if(addrHolder.compareAndSet(addrs, newAddrs)) {
                    MemcachedClient memcachedClient = client.get();

                    // Delayed startup
                    if (memcachedClient == null) {
                        if (newAddrs.isEmpty()) {
                            LOG.warn("No cache servers for %s found, can not start memcached!", cacheName);
                        }
                        else {
                            try {
                                memcachedClient = new MemcachedClient(connectionFactory, newAddrs);
                                if(!client.compareAndSet(null, memcachedClient)) {
                                    LOG.info("Starting delayed memcached client for cache %s on %s", cacheName, newAddrs);
                                }
                                else {
                                    LOG.warn("Wanted to start memcached client, but someone else beat me to it. Should never happen!");
                                    memcachedClient.shutdown();
                                }
                            }
                            catch (IOException ioe) {
                                LOG.warn(ioe, "Could not create memcached client for %s", newAddrs);
                            }
                        }
                    }
                    // Topology change
                    else {
                        final NessKetamaNodeLocator locator = connectionFactory.getNodeLocator();
                        final Map<String, MemcachedNode> nodeKeys = locator.getNodeKeys();
                        final List<InetSocketAddress> nodesToCreate = new ArrayList<InetSocketAddress>();
                        final List<MemcachedNode> shutdownNodes = new ArrayList<MemcachedNode>(locator.getAll());
                        final List<MemcachedNode> newNodes = new ArrayList<MemcachedNode>();

                        for (InetSocketAddress newAddr : newAddrs) {
                            final String newKey = NessKetamaNodeLocator.getKey(newAddr);
                            MemcachedNode newNode = nodeKeys.get(newKey);
                            if (newNode != null) {
                                shutdownNodes.remove(newNode);
                                newNodes.add(newNode);
                                LOG.trace("Found %s, keeping in the ring", newKey);
                            }
                            else {
                                LOG.debug("Created a new connection for %s", newAddr);
                                nodesToCreate.add(newAddr);
                            }
                        }

                        if (!nodesToCreate.isEmpty()) {
                            try {
                                final List<MemcachedNode> createdNodes = memcachedClient.createConnections(nodesToCreate);
                                newNodes.addAll(createdNodes);
                            }
                            catch (IOException ioe) {
                                LOG.warn(ioe, "Could not create new memcached nodes (%s)", nodesToCreate);
                            }
                        }

                        locator.updateLocator(newNodes);
                        for (MemcachedNode oldNode : shutdownNodes) {
                            LOG.debug("Shut down node %s", oldNode.getSocketAddress());
                            connectionFactory.destroyMemcachedNode(oldNode);
                        }
                        final int topologyCount = topologyGeneration.incrementAndGet();
                        LOG.debug("Topology change for %s, generation is now %d, servers: %s", cacheName, topologyCount, locator.getAll());
                    }
                }
                else {
                    LOG.warn("Tried to update cache address to %s, but topology changed behind my back!", newAddrs);
                }
            }
        }
    }


    void waitTopologyChange(final int generation) throws InterruptedException
    {
        while (topologyGeneration.get() <=  generation) {
            Thread.sleep(10L);
        }
    }

    public int getTopologyGeneration()
    {
        return topologyGeneration.get();
    }
}
