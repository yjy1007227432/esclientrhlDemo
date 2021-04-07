package org.zxp.esclientrhl.demo.domain;

import org.zxp.esclientrhl.annotation.ESMetaData;

/**
 * @program: esclientrhlDemo
 * @description:
 * @author: X-Pacific zhang
 * @create: 2021-04-07 14:58
 **/
@ESMetaData(indexName = "alias_demo",
        number_of_shards = 1,
        number_of_replicas = 0,
        printLog = true,
        alias = true,
        aliasIndex = {"alias_demo-01","alias_demo-02","alias_demo-03"},
        writeIndex = "alias_demo-02")
public class AliasDemo {
    private String log;

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public AliasDemo() {

    }

    public AliasDemo(String log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return "AliasDemo{" +
                "log='" + log + '\'' +
                '}';
    }
}
