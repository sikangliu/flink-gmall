package com.lsk.gmall.service;

import com.lsk.gmall.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Desc: 访客流量统计 Mapper
 */
public interface VisitorStatsService {
    //新老访客流量统计
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct," +
            "sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
            "from visitor_stats_2021 where toYYYYMMDD(stt)=#{date} group by is_new")
    public List<VisitorStats> getVisitorStatsByNewFlag(int date);

    //分时流量统计
    @Select("select sum(if(is_new='1', visitor_stats_2021.uv_ct,0)) new_uv,toHour(stt) hr," +
            "sum(visitor_stats_2021.uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(uj_ct) uj_ct " +
            "from visitor_stats_2021 where toYYYYMMDD(stt)=#{date} group by toHour(stt)")
    public List<VisitorStats> getVisitorStatsByHour(int date);

    @Select("select count(pv_ct) pv_ct from visitor_stats_2021 " +
            "where toYYYYMMDD(stt)=#{date} ")
    public Long getPv(int date);

    @Select("select count(uv_ct) uv_ct from visitor_stats_2021 " +
            "where toYYYYMMDD(stt)=#{date} ")
    public Long getUv(int date);
}