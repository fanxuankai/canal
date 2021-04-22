package com.fanxuankai.canal.elasticsearch;

import cn.hutool.core.util.ClassUtil;
import com.fanxuankai.canal.elasticsearch.annotation.Index;
import com.fanxuankai.canal.elasticsearch.annotation.Indexes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class IndexScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexScanner.class);

    public static Set<Class<?>> scan(List<String> basePackages) {
        if (basePackages.isEmpty()) {
            return Collections.emptySet();
        }
        long start = System.currentTimeMillis();
        Set<Class<?>> indexesClasses = basePackages.stream()
                .flatMap(basePackage -> ClassUtil.scanPackageByAnnotation(basePackage, Indexes.class).stream())
                .collect(Collectors.toSet());
        long t = System.currentTimeMillis() - start;
        String simpleName = Index.class.getSimpleName();
        LOGGER.info("Finished {} scanning in {}ms. Found {} {} interfaces.", simpleName, t, indexesClasses.size(),
                simpleName);
        return indexesClasses;
    }

}
