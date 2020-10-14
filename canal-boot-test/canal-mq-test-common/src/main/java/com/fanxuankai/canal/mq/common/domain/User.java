package com.fanxuankai.canal.mq.common.domain;

import com.fanxuankai.canal.core.annotation.CanalTable;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
@CanalTable(schema = "canal_client_example", name = "t_user")
public class User {
    private Long id;
    private Long createdBy;
    private LocalDateTime createDate;
    private Long lastModifiedBy;
    private LocalDateTime lastModifiedDate;
    private Integer version;
    private String phone;
    private String username;
    private String password;
    private Integer type;
    private Boolean deleted;
}