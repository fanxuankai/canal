package com.fanxuankai.canal.test.domain;

import com.fanxuankai.canal.core.annotation.CanalTable;
import com.fanxuankai.canal.elasticsearch.MasterDocumentFunction;
import com.fanxuankai.canal.elasticsearch.annotation.Index;
import com.fanxuankai.canal.elasticsearch.annotation.Indexes;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.time.LocalDateTime;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
@CanalTable(schema = "canal_client_example", name = "t_user")
@Document(indexName = "canal_client_example.user_info", type = "doc")
@Indexes({
        @Index(documentClass = User.class, documentFunctionClass = User.class)
})
public class User implements MasterDocumentFunction<User, User> {
    @Id
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

    @Override
    public User applyForInsert(User insert) {
        return insert;
    }

    @Override
    public User applyForUpdate(User before, User after) {
        return after;
    }

    @Override
    public String applyForDelete(User delete) {
        return delete.getId().toString();
    }
}
