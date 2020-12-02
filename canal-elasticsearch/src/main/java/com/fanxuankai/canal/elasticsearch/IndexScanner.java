package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.elasticsearch.annotation.Index;
import com.fanxuankai.canal.elasticsearch.annotation.Indexes;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author fanxuankai
 */
public class IndexScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexScanner.class);

    public static Set<Class<?>> scan(List<String> basePackages) {
        Reflections r =
                new Reflections(new ConfigurationBuilder()
                        .forPackages(basePackages.toArray(new String[0]))
                        .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner())
                );
        long start = System.currentTimeMillis();
        Set<Class<?>> indexesClasses = Collections.emptySet();
        try {
            indexesClasses = r.getTypesAnnotatedWith(Indexes.class);
        } catch (Exception e) {
            LOGGER.warn(e.getLocalizedMessage());
        }
        long t = System.currentTimeMillis() - start;
        String simpleName = Index.class.getSimpleName();
        LOGGER.info("Finished {} scanning in {}ms. Found {} {} interfaces.", simpleName, t, indexesClasses.size(),
                simpleName);
        return indexesClasses;
    }

}
