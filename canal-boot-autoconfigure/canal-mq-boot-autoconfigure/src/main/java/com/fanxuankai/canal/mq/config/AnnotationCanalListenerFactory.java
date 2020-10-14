package com.fanxuankai.canal.mq.config;

import com.fanxuankai.canal.mq.core.annotation.CanalListener;
import com.fanxuankai.canal.mq.core.listener.AbstractCanalListenerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;

/**
 * @author fanxuankai
 */
public class AnnotationCanalListenerFactory extends AbstractCanalListenerFactory implements ApplicationContextAware {

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
        init(applicationContext.getBeansWithAnnotation(CanalListener.class).values());
    }

}
