package ness.cache2;

import java.util.Map;

interface CacheStatisticsManager {
    Map<String, CacheStatistics> getCacheStatistics();
    CacheStatistics getCacheStatistics(String namespace);
}
