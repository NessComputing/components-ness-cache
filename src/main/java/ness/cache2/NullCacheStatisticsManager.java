package ness.cache2;

import java.util.Collections;
import java.util.Map;

import com.google.inject.Inject;

class NullCacheStatisticsManager implements CacheStatisticsManager {
    @Inject
    NullCacheStatisticsManager() { }

    @Override
    public Map<String, CacheStatistics> getCacheStatistics() {
        return Collections.emptyMap();
    }

    @Override
    public CacheStatistics getCacheStatistics(String namespace) {
        return new CacheStatistics(namespace);
    }
}
