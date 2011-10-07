package ness.cache2;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleListener;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.log.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import net.spy.memcached.MemcachedClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

/**
 * Maintain a {@link MemcachedClient} which is always connected to the currently operating
 * memcached cluster.  Periodically uses the {@link CacheTopologyProvider} and if there is a change,
 * recreate the client.
 */
@Singleton
@ThreadSafe
class MemcachedClientFactory {
    private static final Log LOG = Log.findLog();
    private volatile MemcachedClient client;
    private volatile List<InetSocketAddress> memcachedCluster = ImmutableList.of();
    private final ScheduledExecutorService clientReconfigurationService;
    private final CacheTopologyProvider cacheTopology;

    private volatile CountDownLatch awaitLatch = new CountDownLatch(1);
    private final Provider<NessMemcachedConnectionFactory> connectionFactoryProvider;

    @Inject
    MemcachedClientFactory(
            final CacheConfiguration configuration,
            Lifecycle lifecycle,
            CacheTopologyProvider cacheTopology,
            Provider<NessMemcachedConnectionFactory> connectionFactoryProvider) {

        this.cacheTopology = cacheTopology;
        this.connectionFactoryProvider = connectionFactoryProvider;
        clientReconfigurationService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("memcached-discovery-%d").build());

        lifecycle.addListener(LifecycleStage.START_STAGE, new LifecycleListener() {
            @Override
            public void onStage(LifecycleStage lifecycleStage) {
                LOG.info("Kicking off memcache topology discovery thread");
                MemcachedDiscoveryUpdate updater = new MemcachedDiscoveryUpdate();

                // Run update once before anything else happens so we don't
                // observe a null client after the lifecycle starts
                updater.run();

                clientReconfigurationService.scheduleAtFixedRate(
                        updater,
                        configuration.getCacheServerRediscoveryInterval().getMillis(),
                        configuration.getCacheServerRediscoveryInterval().getMillis(), TimeUnit.MILLISECONDS);
            }
        });

        lifecycle.addListener(LifecycleStage.STOP_STAGE, new LifecycleListener() {
            @Override
            public void onStage(LifecycleStage lifecycleStage) {
                clientReconfigurationService.shutdown();
                // Since the executor service is shutdown, no more updates may happen.  So this client
                // is the final one that will be created, and client will not be concurrently modified
                MemcachedClient clientToShutdown;
                if ((clientToShutdown = client) != null) {
                    client = null; // Prevent further operations
                    clientToShutdown.shutdown(30, TimeUnit.SECONDS); // Shut down gracefully
                }
                LOG.info("Caching system stopped");
            }
        });
    }

    // This method must be very cheap, it is called per-operation
    public MemcachedClient get() {
        return client;
    }

    private class MemcachedDiscoveryUpdate implements Runnable {
        @Override
        public void run() {
            // Guaranteed to not run multiply so no additional coordination is required; see ScheduledExecutorService.
            LOG.trace("Cache prior to discovery = %s", client);

            // Discover potentially new cluster topology
            List<InetSocketAddress> addrs = ImmutableList.copyOf(cacheTopology.get());

            if (!memcachedCluster.equals(addrs)) {
                memcachedCluster = addrs;

                try {
                    final MemcachedClient newClient;
                    if (memcachedCluster.isEmpty()) {
                        newClient = null;
                    } else {
                        newClient = new MemcachedClient(connectionFactoryProvider.get(), addrs);
                    }

                    MemcachedClient oldClient = client;
                    client = newClient;

                    LOG.info("Discovery complete, new client talks to %s -> %s", addrs, newClient);
                    if (oldClient != null) {
                        LOG.debug("Shutting down old client");
                        oldClient.shutdown(100, TimeUnit.MILLISECONDS);
                        LOG.trace("Old client shutdown");
                    }

                    awaitLatch.countDown();

                    if (client == null) {
                        LOG.warn("WARNING: all memcached servers disappeared!");
                    }
                } catch (IOException e) {
                    LOG.error(e, "Could not connect to memcached cluster");
                }
            } else {
                LOG.trace("No cache update due to identical cluster configuration %s", memcachedCluster);
            }
        }
    }

    void readyAwaitTopologyChange() {
        awaitLatch = new CountDownLatch(1);
    }

    /**
     * Wait for the cache topology to change.  ONLY SUITABLE FOR USE IN UNIT TESTS.
     * USING THIS METHOD FROM ANY THREAD OTHER THAN THE MAIN JUNIT THREAD WILL INVOKE
     * UNDEFINED BEHAVIOR.
     */
    void awaitTopologyChange() throws InterruptedException {
        LOG.info("Begin awaiting topology change");
        Preconditions.checkState(awaitLatch.await(5, TimeUnit.SECONDS), "state did not change");
        LOG.info("Finished awaiting topology change");
    }
}
