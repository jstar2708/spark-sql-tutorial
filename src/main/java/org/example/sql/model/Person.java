package org.example.sql.model;

import java.io.Serializable;

public class Person implements Serializable {
    private String name;

    public Person(String name, long age) {
        this.name = name;
        this.age = age;
    }

    public Person() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    private long age;
}
