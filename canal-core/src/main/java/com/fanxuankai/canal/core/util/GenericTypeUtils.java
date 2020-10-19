package com.fanxuankai.canal.core.util;

import org.springframework.core.GenericTypeResolver;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 泛型工具类
 *
 * @author fanxuankai
 */
public class GenericTypeUtils {

    /**
     * 获取泛型
     *
     * @param clazz              子类
     * @param genericDeclaration 父类或接口
     * @param index              泛型索引
     * @param <T>                泛型类型
     * @return 泛型的具体类型
     * @throws IndexOutOfBoundsException index 越界
     */
    public static <T> Class<T> getGenericType(Class<?> clazz, Class<?> genericDeclaration, int index) {
        List<Type> allTypes = getAllTypes(clazz, genericDeclaration);
        return getGenericClass(allTypes.get(index));
    }

    /**
     * 获取泛型
     *
     * @param clazz              子类
     * @param genericDeclaration 父类或接口
     * @param name               泛型名
     * @param <T>                泛型类型
     * @return 泛型的具体类型
     */
    public static <T> Class<T> getGenericType(Class<?> clazz, Class<?> genericDeclaration, String name) {
        Type type = getAllTypeMap(clazz, genericDeclaration).get(name);
        return getGenericClass(type);
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> getGenericClass(Type type) {
        if (type instanceof Class<?>) {
            return (Class<T>) type;
        }

        if (type instanceof ParameterizedType) {
            Type rawType = ((ParameterizedType) type).getRawType();
            if (rawType instanceof Class<?>) {
                return (Class<T>) rawType;
            } else {
                return getGenericClass(rawType);
            }
        }

        throw new ClassCastException();
    }

    private static List<Type> getAllTypes(Class<?> clazz, Class<?> genericDeclaration) {
        return GenericTypeResolver.getTypeVariableMap(clazz)
                .entrySet()
                .stream()
                .filter(entry -> genericDeclaration.isAssignableFrom((Class<?>) entry.getKey().getGenericDeclaration()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    private static Map<String, Type> getAllTypeMap(Class<?> clazz, Class<?> genericDeclaration) {
        return GenericTypeResolver.getTypeVariableMap(clazz)
                .entrySet()
                .stream()
                .filter(entry -> genericDeclaration.isAssignableFrom((Class<?>) entry.getKey().getGenericDeclaration()))
                .collect(Collectors.toMap(o -> o.getKey().getName(), Map.Entry::getValue));
    }

}
