package com.avricot.horm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.TreeSet;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface HormList {
    public Class<?> getKlass();

    public Set<?> test = new TreeSet<Comparable<?>>();
}
