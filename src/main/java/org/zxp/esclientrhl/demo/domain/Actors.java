package org.zxp.esclientrhl.demo.domain;

import org.zxp.esclientrhl.annotation.ESMapping;

/**
 * @program: esdemo
 * @description:
 * @author: X-Pacific zhang
 * @create: 2020-01-01 11:24
 **/
public class Actors {
    @ESMapping
    private String first_name;
    @ESMapping
    private String last_name;

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    @Override
    public String toString() {
        return "Actors{" +
                "first_name='" + first_name + '\'' +
                ", last_name='" + last_name + '\'' +
                '}';
    }
}
