package com.fanxuankai.canal.test.domain;

import com.fanxuankai.canal.core.annotation.CanalTable;
import com.fanxuankai.canal.elasticsearch.MasterDocumentFunction;
import com.fanxuankai.canal.elasticsearch.annotation.Index;
import com.fanxuankai.canal.elasticsearch.annotation.Indexes;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.time.LocalDateTime;

/**
 * @author fanxuankai
 */
@CanalTable(schema = "canal_client_example", name = "t_user")
@Document(indexName = "canal_client_example.user_info")
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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(Long createdBy) {
        this.createdBy = createdBy;
    }

    public LocalDateTime getCreateDate() {
        return createDate;
    }

    public void setCreateDate(LocalDateTime createDate) {
        this.createDate = createDate;
    }

    public Long getLastModifiedBy() {
        return lastModifiedBy;
    }

    public void setLastModifiedBy(Long lastModifiedBy) {
        this.lastModifiedBy = lastModifiedBy;
    }

    public LocalDateTime getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(LocalDateTime lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
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

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

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
