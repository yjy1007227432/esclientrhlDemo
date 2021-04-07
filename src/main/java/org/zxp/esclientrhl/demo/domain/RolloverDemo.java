package org.zxp.esclientrhl.demo.domain;

import org.zxp.esclientrhl.annotation.ESID;
import org.zxp.esclientrhl.annotation.ESMapping;
import org.zxp.esclientrhl.annotation.ESMetaData;
import org.zxp.esclientrhl.enums.DataType;


/**
 * @program: esdemo
 * @description:
 * @author: X-Pacific zhang
 * @create: 2020-09-08 19:15
 **/
@ESMetaData(indexName = "rollover_demo",
        number_of_shards = 1,
        number_of_replicas = 0,
        printLog = true,
        rollover = true,
        rolloverMaxIndexDocsCondition = 2)
public class RolloverDemo {
    @ESID
    private String uid;
    @ESMapping(datatype = DataType.keyword_type)
    private String  log;

    @Override
    public String toString() {
        return "TestRollover{" +
                "uid='" + uid + '\'' +
                ", log='" + log + '\'' +
                '}';
    }


    public RolloverDemo() {
    }

    public RolloverDemo(String uid, String log) {
        this.uid = uid;
        this.log = log;
    }

    public RolloverDemo(String log) {
        this.log = log;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }
}
