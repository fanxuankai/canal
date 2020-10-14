package com.fanxuankai.canal.xxlmq;

import com.fanxuankai.canal.mq.core.model.ListenerMetadata;
import com.xxl.mq.client.consumer.annotation.MqConsumer;
import com.xxl.mq.client.consumer.thread.ConsumerThread;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.IntegerMemberValue;
import javassist.bytecode.annotation.StringMemberValue;
import org.springframework.util.StringUtils;

import java.util.Optional;
import java.util.UUID;

/**
 * @author fanxuankai
 */
public class MqConsumerHelper {

    /**
     * 动态生成 class, 并且加上 @MqConsumer 注解
     *
     * @param listenerMetadata the listener metadata
     * @return new class
     */
    public static Class<?> newClass(ListenerMetadata listenerMetadata) {
        try {
            ClassPool pool = ClassPool.getDefault();
            String templateClassname = XxlMqConsumer.class.getName();
            pool.insertClassPath(new ClassClassPath(XxlMqConsumer.class));
            String topic = listenerMetadata.getTopic();
            String classname = templateClassname + UUID.randomUUID().toString().replaceAll("-", "") + "@" + topic;
            CtClass clazz = pool.makeClass(classname, pool.getCtClass(templateClassname));
            ClassFile classFile = clazz.getClassFile();
            ConstPool constPool = classFile.getConstPool();
            Annotation classAnnotation = new Annotation(MqConsumer.class.getName(), constPool);
            String group = listenerMetadata.getGroup();
            if (StringUtils.hasText(group)) {
                classAnnotation.addMemberValue("group", new StringMemberValue(group, constPool));
            } else {
                classAnnotation.addMemberValue("group", new StringMemberValue(MqConsumer.DEFAULT_GROUP, constPool));
            }
            classAnnotation.addMemberValue("topic", new StringMemberValue(topic, constPool));
            classAnnotation.addMemberValue("waitRateSeconds", new IntegerMemberValue(constPool,
                    Optional.of(listenerMetadata.getWaitRateSeconds()).orElse(ConsumerThread.DEFAULT_WAIT_RATE_SECONDS)));
            classAnnotation.addMemberValue("waitMaxSeconds", new IntegerMemberValue(constPool,
                    Optional.of(listenerMetadata.getWaitMaxSeconds()).orElse(ConsumerThread.DEFAULT_WAIT_MAX_SECONDS)));
            AnnotationsAttribute classAttribute = new AnnotationsAttribute(constPool, AnnotationsAttribute.visibleTag);
            classAttribute.addAnnotation(classAnnotation);
            clazz.getClassFile().addAttribute(classAttribute);
            return clazz.toClass();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}