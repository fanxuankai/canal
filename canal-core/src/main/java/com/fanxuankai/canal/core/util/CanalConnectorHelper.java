package com.fanxuankai.canal.core.util;

import cn.hutool.core.thread.ThreadUtil;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Canal 连接帮助类
 *
 * @author fanxuankai
 */
public class CanalConnectorHelper implements InitializingBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConnectorHelper.class);
    private String destination;
    private String filter;
    private String zkServers;
    private String hostname;
    private String username;
    private String password;
    private Integer port;

    private CanalConnector canalConnector;

    @Override
    public void afterPropertiesSet() {
        this.canalConnector = createConnect();
    }

    /**
     * 创建连接
     *
     * @return CanalConnector
     */
    private CanalConnector createConnect() {
        CanalConnector canalConnector;
        if (StringUtils.hasText(zkServers)) {
            canalConnector = CanalConnectors.newClusterConnector(zkServers,
                    destination, username, password);
        } else {
            canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination,
                    username, password);
        }
        this.canalConnector = canalConnector;
        tryConnect();
        return canalConnector;
    }

    /**
     * 链接
     */
    private void tryConnect() {
        // 异常后重试
        while (true) {
            try {
                canalConnector.connect();
                canalConnector.subscribe(filter);
                canalConnector.rollback();
                return;
            } catch (CanalClientException e) {
                if ("java.nio.channels.ClosedByInterruptException".equals(e.getLocalizedMessage())) {
                    Thread.currentThread().interrupt();
                    return;
                }
                LOGGER.error(destination, e);
                ThreadUtil.sleep(2, TimeUnit.SECONDS);
            }
        }
    }

    public void disconnect() {
        if (canalConnector != null) {
            canalConnector.unsubscribe();
            canalConnector.disconnect();
        }
    }

    public void reconnect() {
        afterPropertiesSet();
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
