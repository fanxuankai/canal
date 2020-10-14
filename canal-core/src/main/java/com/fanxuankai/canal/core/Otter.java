package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.client.CanalConnector;

/**
 * Otter 客户端接口
 *
 * @author fanxuankai
 */
public interface Otter {

    /**
     * 开启
     */
    void start();

    /**
     * 停止
     */
    void stop();

    /**
     * CanalConnector
     *
     * @return the CanalConnector
     */
    CanalConnector getCanalConnector();
}
