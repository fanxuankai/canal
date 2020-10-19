package com.fanxuankai.canal.core.config;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.util.StringUtils;

/**
 * canal参数配置
 *
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class CanalConfiguration {

    /**
     * 应用 id, 用于 canal client 抢占式运行、binlog offset 防重, 默认取 instance
     */
    private String id;

    /**
     * 集群配置
     */
    private Cluster cluster;

    /**
     * 单节点配置
     */
    private SingleNode singleNode = new SingleNode();

    /**
     * 实例
     */
    private String instance = "example";

    /**
     * 过滤
     */
    private String filter = ".*\\..*";

    /**
     * 账号
     */
    private String username = "canal";

    /**
     * 密码
     */
    private String password = "canal";

    /**
     * 拉取数据的间隔 ms
     */
    private long intervalMillis = 1_000;

    /**
     * 拉取数据的数量
     */
    private int batchSize = 100;

    /**
     * 打印事件日志
     */
    private boolean showEventLog = true;

    /**
     * 打印 Entry 日志
     */
    private boolean showEntryLog = true;

    /**
     * 打印数据明细日志
     */
    private boolean showRowChange;

    /**
     * 格式化数据明细日志
     */
    private boolean formatRowChangeLog;

    /**
     * 批次达到一定数量进行并行处理, 且确保顺序消费
     */
    private int performanceThreshold = 10_000;

    /**
     * 跳过处理
     */
    private boolean skip;

    /**
     * 全局逻辑删除字段
     */
    private String logicDeleteField = "deleted";

    /**
     * 激活逻辑删除
     */
    private boolean enableLogicDelete;
    /**
     * 抢占式运行参数
     */
    private Preemptive preemptive = new Preemptive();

    public String getId() {
        return StringUtils.hasText(id) ? id : instance;
    }

    @Data
    public static class Cluster {
        /**
         * zookeeper host:port
         */
        private String nodes = "localhost:2181,localhost:2182,localhost:2183";
    }

    @Data
    public static class SingleNode {
        /**
         * host
         */
        private String hostname = "localhost";
        /**
         * port
         */
        private int port = 11111;
    }

    @Data
    public static class Preemptive {
        /**
         * 心跳超时 s
         */
        private Long timeout = 15L;
        /**
         * 心跳频率 s
         */
        private Long keep = 5L;
        /**
         * ping 的频率 s
         */
        private Long ping = 30L;
    }

}
