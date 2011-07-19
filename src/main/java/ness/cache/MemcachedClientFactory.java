package ness.cache;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleListener;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.log.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
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

    @Inject
    MemcachedClientFactory(final CacheConfiguration configuration, Lifecycle lifecycle, CacheTopologyProvider cacheTopology) {

        this.cacheTopology = cacheTopology;
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
            List<InetSocketAddress> addrs = cacheTopology.get();

            if (!memcachedCluster.equals(addrs)) {
                memcachedCluster = addrs;

                try {

                    MemcachedClient newClient = new MemcachedClient(new BinaryConnectionFactory() {
                        @SuppressWarnings(value = {"unchecked", "rawtypes"} )
                        @Override
                        public Transcoder<Object> getDefaultTranscoder() {
                            return (Transcoder) new MemcacheByteArrayTranscoder();
                        }
                    }, addrs);

                    client = newClient;
                    LOG.debug("Discovery complete, new client talks to %s -> %s", addrs, newClient);
                } catch (IOException e) {
                    LOG.error(e, "Could not connect to memcached cluster");
                }
            } else {
                LOG.trace("No cache update due to identical cluster configuration %s", memcachedCluster);
            }
        }
    }
}
