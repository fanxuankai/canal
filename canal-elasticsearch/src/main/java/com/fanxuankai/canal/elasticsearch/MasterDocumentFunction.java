package com.fanxuankai.canal.elasticsearch;

/**
 * 主表构建文档函数
 *
 * @param <T> 实体类泛型
 * @param <D> 文档泛型
 * @author fanxuankai
 */
public interface MasterDocumentFunction<T, D> extends DocumentFunction<T, D> {

    /**
     * 新增事件函数
     *
     * @param insert 实体类对象
     * @return 文档对象
     */
    D applyForInsert(T insert);

    /**
     * 修改事件函数
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
     * @return 文档 id
     */
    String applyForDelete(T delete);

}
