package com.fanxuankai.canal.elasticsearch;

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
     * 修改事件函数
     *
     * @param before 实体类对象
     * @param after  实体类对象
     * @return 文档对象list
     */
    List<D> applyForUpdate(T before, T after);

    /**
     * 删除事件函数
     *
     * @param delete 实体类对象
     * @return 文档对象list
     */
    List<D> applyForDelete(T delete);

}
