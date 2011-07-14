package ness.cache;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleListener;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.log.Log;

import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.weakref.jmx.MBeanExporter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Manage various {@link CacheStatistics} beans exported via JMX.  Exports them on first access,
 * and unexports them on lifecycle shutdown.
 */
@Singleton
@ThreadSafe
class CacheStatisticsManager {
    private static final Log LOG = Log.findLog();
    private final Lifecycle lifecycle;
    private final MBeanExporter exporter;
    private final boolean jmxEnabled;
    @GuardedBy("this")
    private final Map<String, CacheStatistics> statistics = Maps.newHashMap();

    @Inject
    CacheStatisticsManager(Lifecycle lifecycle, MBeanExporter exporter, CacheConfiguration config) {
        this.lifecycle = lifecycle;
        this.exporter = exporter;
        this.jmxEnabled = config.isJmxEnabled();
    }

    public synchronized Map<String, CacheStatistics> getCacheStatistics() {
        return ImmutableMap.copyOf(statistics);
    }

    public synchronized CacheStatistics getCacheStatistics(String namespace) {
        CacheStatistics result = statistics.get(namespace);
        if (result == null) {
            result = new CacheStatistics(namespace);

            LOG.debug("Initializing statistics for new cache namespace %s", namespace);

            if (jmxEnabled) {
                final String objectName = "ness.cache:namespace=" + namespace;
                exporter.export(objectName, result);
                lifecycle.addListener(LifecycleStage.STOP_STAGE, new LifecycleListener() {
                    @Override
                    public void onStage(LifecycleStage lifecycleStage) {
                        exporter.unexport(objectName);
                    }
                });
            }

            statistics.put(namespace, result);
        }
        return result;
    }
}
