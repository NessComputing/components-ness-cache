package ness.cache2;

import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.OnStage;
import com.likeness.logging.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import net.spy.memcached.MemcachedClient;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

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

    private final Provider<NessMemcachedConnectionFactory> connectionFactoryProvider;
    private final String cacheName;
    private final CacheConfiguration configuration;

    @Inject
    MemcachedClientFactory(final CacheConfiguration configuration,
                           final CacheTopologyProvider cacheTopology,
                           final Provider<NessMemcachedConnectionFactory> connectionFactoryProvider,
                           @Nullable @Named("cacheName") final String cacheName)
    {

        this.cacheTopology = cacheTopology;
        this.cacheName = Objects.firstNonNull(cacheName, "<default>");
        this.configuration = configuration;

        this.connectionFactoryProvider = connectionFactoryProvider;
    }

    @OnStage(LifecycleStage.START)
    public void start()
    {
        Preconditions.checkState(clientReconfigurationService.get() == null, "client is already started!");

        final ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("memcached-discovery-" + cacheName).setDaemon(true).build();
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, tf);
        if(clientReconfigurationService.compareAndSet(null, executor)) {
            LOG.info("Kicking off memcache topology discovery thread");
            final MemcachedDiscoveryUpdate updater = new MemcachedDiscoveryUpdate();

            // Run update once before anything else happens so we don't
            // observe a null client after the lifecycle starts
            updater.run();

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
                    MemcachedClient oldClient = null;

                    try {
                        final MemcachedClient newClient;
                        if (newAddrs.isEmpty()) {
                            newClient = null;
                            LOG.warn("All memcached servers disappeared!");
                        }
                        else {
                            newClient = new MemcachedClient(connectionFactoryProvider.get(), newAddrs);
                        }

                        oldClient = client.getAndSet(newClient);
                        final int topologyCount = topologyGeneration.incrementAndGet();

                        LOG.debug("Topology change for %s, generation is now %d, client: %s", cacheName, topologyCount, newClient);
                    }
                    catch (IOException ioe) {
                        LOG.errorDebug(ioe, "Could not connect to memcached cluster %s", cacheName);
                    }
                    finally {
                        if (oldClient != null) {
                            LOG.debug("Shutting down old client");
                            oldClient.shutdown(100, TimeUnit.MILLISECONDS);
                            LOG.trace("Old client shutdown");
                        }
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
