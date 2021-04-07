package org.zxp.esclientrhl.demo.domain;

import org.zxp.esclientrhl.annotation.ESMapping;
import org.zxp.esclientrhl.annotation.ESMetaData;
import org.zxp.esclientrhl.enums.DataType;
import org.zxp.esclientrhl.repository.GeoEntity;

/**
 * @program: esdemo
 * @description:
 * @author: X-Pacific zhang
 * @create: 2020-09-10 08:44
 **/
@ESMetaData(indexName = "deo_demo",
        number_of_shards = 1,
        number_of_replicas = 0,
        printLog = true
)
public class GeoDemo {
    @ESMapping(datatype = DataType.long_type)
    Long userId;
    @ESMapping(datatype = DataType.text_type)
    String userName;
    @ESMapping(datatype = DataType.geo_point_type)
    GeoEntity geo;
    @ESMapping(datatype = DataType.text_type)
    String place;

    @Override
    public String toString() {
        return "GeoPojo{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                ", geo=" + geo +
                ", place='" + place + '\'' +
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public GeoEntity getGeo() {
        return geo;
    }

    public void setGeo(GeoEntity geo) {
        this.geo = geo;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }
}
