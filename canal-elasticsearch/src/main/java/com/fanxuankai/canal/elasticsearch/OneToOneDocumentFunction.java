package com.fanxuankai.canal.elasticsearch;

/**
 * 与主表的关系 1:1
 *
 * @param <T> 实体类泛型
 * @param <D> 文档泛型
 * @author fanxuankai
 */
public interface OneToOneDocumentFunction<T, D> extends DocumentFunction<T, D> {

    /**
     * 删除事件函数
     *
     * @param before 实体类对象
     * @param after  实体类对象
     * @return 文档对象
     */
    D applyForUpdate(T before, T after);

    /**
     * 删除事件函数
     *
     * @param delete 实体类对象
     * @return 文档对象
     */
    D applyForDelete(T delete);

}
