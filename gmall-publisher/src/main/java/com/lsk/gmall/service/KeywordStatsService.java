package com.lsk.gmall.service;

import com.lsk.gmall.bean.KeywordStats;

import java.util.List;
/**
 * Desc: 关键词统计接口
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}
