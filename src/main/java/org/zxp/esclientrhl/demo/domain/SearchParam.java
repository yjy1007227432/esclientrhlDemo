package org.zxp.esclientrhl.demo.domain;

import lombok.Data;

import java.util.List;


@Data
public class SearchParam {
    private String operator;
    private String type;
    private String column;
    private Object val;
    private List<Or> ors;

    @Data
    public static class Or{
        private String type;
        private String column;
        private Object val;
    }
}
