package ness.cache;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleListener;
import io.trumpet.lifecycle.LifecycleStage;

import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.weakref.jmx.MBeanExporter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
@ThreadSafe
class CacheStatisticsManager {
    private final Lifecycle lifecycle;
    @GuardedBy("this")
    private final Map<String, CacheStatistics> statistics = Maps.newHashMap();
    private final MBeanExporter exporter;
    
    @Inject
    CacheStatisticsManager(Lifecycle lifecycle, MBeanExporter exporter) {
        this.lifecycle = lifecycle;
        this.exporter = exporter;
    }
    
    public synchronized Map<String, CacheStatistics> getCacheStatistics() {
        return ImmutableMap.copyOf(statistics);
    }
    
    public synchronized CacheStatistics getCacheStatistics(String namespace) {
        CacheStatistics result = statistics.get(namespace);
        if (result == null) {
            result = new CacheStatistics(namespace);
            final String objectName = "ness.cache:namespace=" + namespace;
            exporter.export(objectName, result);
            lifecycle.addListener(LifecycleStage.STOP_STAGE, new LifecycleListener() {
                @Override
                public void onStage(LifecycleStage lifecycleStage) {
                    exporter.unexport(objectName);
                }
            });
            statistics.put(namespace, result);
        }
        return result;
    }
}
