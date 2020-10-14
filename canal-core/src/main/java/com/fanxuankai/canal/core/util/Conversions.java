package com.fanxuankai.canal.core.util;

import com.alibaba.fastjson.JSONObject;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * 转换工具类
 *
 * @author fanxuankai
 */
public class Conversions {
    private static DefaultConversionService cs;

    /**
     * 获取实例
     *
     * @return 单例
     */
    public static ConversionService getInstance() {
        if (cs == null) {
            synchronized (Conversions.class) {
                if (cs == null) {
                    cs = new DefaultConversionService();
                    cs.addConverter(new StringToDateConverter());
                    cs.addConverter(new StringToLocalDateConverter());
                    cs.addConverter(new StringToLocalDateTimeConverter());
                }
            }
        }
        return cs;
    }

    private static class StringToDateConverter extends FastJsonConverter<Date> {
        public StringToDateConverter() {
            super(Date.class);
        }
    }

    private static class StringToLocalDateConverter extends FastJsonConverter<LocalDate> {
        public StringToLocalDateConverter() {
            super(LocalDate.class);
        }
    }

    private static class StringToLocalDateTimeConverter extends FastJsonConverter<LocalDateTime> {
        public StringToLocalDateTimeConverter() {
            super(LocalDateTime.class);
        }
    }

    private static class FastJsonConverter<T> implements Converter<String, T> {
        private final Class<T> type;

        public FastJsonConverter(Class<T> type) {
            this.type = type;
        }

        @Override
        public T convert(String s) {
            String key = "value";
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(key, s);
            return jsonObject.getObject(key, type);
        }
    }
}
