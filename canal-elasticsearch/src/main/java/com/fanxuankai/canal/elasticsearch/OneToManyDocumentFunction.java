package com.fanxuankai.canal.elasticsearch;

/**
 * 与主表的关系为 1:n
 *
 * @param <T> 实体类泛型
 * @param <D> 文档泛型
 * @author fanxuankai
 */
public interface OneToManyDocumentFunction<T, D> extends DocumentFunction<T, D> {

    /**
     * 新增事件函数
     * 两种情况:
     * 1.为主表的外键,这种情况主表通常还没有数据,无需实现
     * 2.中间表的形式,副表事件时无需更新,中间表事件时需要更新主表的附属信息
     *
     * @param insert 实体类对象
     * @return UpdateByQuery
     */
    default UpdateByQuery applyForInsert(T insert) {
        // nothing
        return null;
    }

    /**
     * 修改事件函数
     * 两种情况:
     * 1.为主表的外键,需要更新主表的附属信息
     * 2.中间表的形式,副表事件时需要更新主表的附属信息;中间表事件时:主表id修改时需要修改前后的主表都需要更新附属信息,副表id修改时只需更新主表的附属信息;否则无需修改
     *
     * @param before 实体类对象
     * @param after  实体类对象
     * @return UpdateByQuery
     */
    UpdateByQuery applyForUpdate(T before, T after);

    /**
     * 删除事件函数
     * 两种情况:
     * 1.为主表的外键,需要更新主表的附属信息
     * 2.中间表的形式,副表事件时无需更新;中间表事件需要更新主表的附属信息
     *
     * @param delete 实体类对象
     * @return UpdateByQuery
     */
    default UpdateByQuery applyForDelete(T delete) {
        // nothing
        return null;
    }

}
