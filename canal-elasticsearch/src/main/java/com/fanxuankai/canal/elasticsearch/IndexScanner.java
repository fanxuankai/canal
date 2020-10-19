package com.fanxuankai.canal.elasticsearch;

import com.fanxuankai.canal.elasticsearch.annotation.Index;
import com.fanxuankai.canal.elasticsearch.annotation.Indexes;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.util.StopWatch;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author fanxuankai
 */
@Slf4j
public class IndexScanner {

    public static Set<Class<?>> scan(List<String> basePackages) {
        Reflections r =
                new Reflections(new ConfigurationBuilder()
                        .forPackages(basePackages.toArray(new String[0]))
                        .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner())
                );
        StopWatch sw = new StopWatch();
        sw.start();
        Set<Class<?>> indexesClasses = Collections.emptySet();
        try {
            indexesClasses = r.getTypesAnnotatedWith(Indexes.class);
        } catch (Exception e) {
            log.warn(e.getLocalizedMessage());
        }
        sw.stop();
        String simpleName = Index.class.getSimpleName();
        log.info("Finished {} scanning in {}ms. Found {} {} interfaces.", simpleName, sw.getTotalTimeMillis(),
                indexesClasses.size(), simpleName);
        return indexesClasses;
    }

}
