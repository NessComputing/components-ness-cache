package ness.cache;

import java.util.Map;

interface CacheStatisticsManager {
    Map<String, CacheStatistics> getCacheStatistics();
    CacheStatistics getCacheStatistics(String namespace);
}
