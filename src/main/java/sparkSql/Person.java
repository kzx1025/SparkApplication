package sparkSql;

import java.io.Serializable;

/**
 * Created by iceke on 16/4/15.
 */
public  class Person implements Serializable {
    private String name;
    private int age;

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }
}