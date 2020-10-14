package com.fanxuankai.canal.core.util;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.fanxuankai.commons.util.concurrent.Threads;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Canal 连接帮助类
 *
 * @author fanxuankai
 */
@Slf4j
public class CanalConnectorHelper {
    private String destination;
    private String filter;
    private String zkServers;
    private String hostname;
    private String username;
    private String password;
    private Integer port;

    private CanalConnector canalConnector;

    /**
     * 连接
     *
     * @return CanalConnector
     */
    public CanalConnector createConnect() {
        CanalConnector canalConnector;
        if (StringUtils.hasText(zkServers)) {
            canalConnector = CanalConnectors.newClusterConnector(zkServers,
                    destination, username, password);
        } else {
            canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination,
                    username, password);
        }
        if (canalConnector == null) {
            throw new IllegalArgumentException("请检查 Canal 链接配置");
        }
        this.canalConnector = canalConnector;
        subscribe();
        return canalConnector;
    }

    /**
     * 链接
     */
    private void subscribe() {
        // 异常后重试
        while (true) {
            try {
                canalConnector.connect();
                canalConnector.subscribe(filter);
                canalConnector.rollback();
                return;
            } catch (CanalClientException e) {
                log.error("链接失败", e);
                Threads.sleep(2, TimeUnit.SECONDS);
            }
        }
    }

    public void disconnect() {
        canalConnector.unsubscribe();
        canalConnector.disconnect();
    }

    public void reconnect() {
        disconnect();
        subscribe();
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getZkServers() {
        return zkServers;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public CanalConnector getCanalConnector() {
        return canalConnector;
    }
}
