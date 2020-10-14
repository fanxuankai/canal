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
     * 修改事件函数
     *
     * @param before 实体类对象
     * @param after  实体类对象
     * @return 文档对象
     */
    UpdateByQuery applyForUpdate(T before, T after);

    /**
     * 删除事件函数
     *
     * @param delete 实体类对象
     * @return 文档对象
     */
    UpdateByQuery applyForDelete(T delete);

}
