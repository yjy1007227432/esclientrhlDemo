package org.zxp.esclientrhl.demo.domain;


import lombok.Data;

@Data
public class BaseResult {

    private String resultCode; //返回码 0：成功，1：失败
    private String resultMsg; //返回消息
    private Object obj; //返回结果

    public String getResultCode() {
        return resultCode;
    }

    public BaseResult() {
        super();
        this.resultCode = "1";
        this.resultMsg = "";
        this.obj = obj;
    }


    @Override
    public String toString() {
        return "BaseResult{" +
                "resultCode=" + resultCode +
                ", resultMsg='" + resultMsg + '\'' +
                ", obj=" + obj +
                '}';
    }
}
