package com.fanxuankai.canal.elasticsearch;

import java.util.Collections;
import java.util.List;

/**
 * 与主表的关系 n:n
 *
 * @param <T> 实体类泛型
 * @param <D> 文档泛型
 * @author fanxuankai
 */
public interface ManyToManyDocumentFunction<T, D> extends DocumentFunction<T, D> {

    /**
     * 新增事件函数
     * 一种情况:
     * 1.中间表的形式,副表事件时无需更新;中间表事件需要更新主表的附属信息
     *
     * @param insert 实体类对象
     * @return 文档对象list
     */
    default List<D> applyForInsert(T insert) {
        // nothing
        return Collections.emptyList();
    }

    /**
     * 修改事件函数
     * 一种情况:
     * 1.中间表的形式,副表事件时需更新主表的附属信息;中间表事件时:主表id修改时需要修改前后的主表都需要更新附属信息,副表id修改时只需更新主表的附属信息;否则无需修改
     *
     * @param before 实体类对象
     * @param after  实体类对象
     * @return 文档对象list
     */
    List<D> applyForUpdate(T before, T after);

    /**
     * 删除事件函数
     * 一种情况:
     * 1.中间表的形式,副表事件时无需更新;中间表事件需要更新主表的附属信息
     *
     * @param delete 实体类对象
     * @return 文档对象list
     */
    default List<D> applyForDelete(T delete) {
        // nothing
        return Collections.emptyList();
    }

}
