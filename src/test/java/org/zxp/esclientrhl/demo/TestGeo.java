package org.zxp.esclientrhl.demo;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.GeoDemo;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;
import org.zxp.esclientrhl.repository.GeoEntity;
import org.zxp.esclientrhl.util.IndexTools;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @program: GEO的使用
 * @description:
 * @author: X-Pacific zhang
 * @create: 2020-09-10 08:48
 **/
public class TestGeo extends  EsclientrhlDemoApplicationTests{
    @Autowired
    ElasticsearchTemplate<GeoDemo,String> elasticsearchTemplate;

    @Autowired
    IndexTools indexTools;

    @Test
    public void testGeo() throws Exception {
        GeoEntity gp1 = new GeoEntity(1,1);
        GeoEntity gp2 = new GeoEntity(2,2);
        GeoEntity gp3 = new GeoEntity(3,3);
        GeoDemo g1 = new GeoDemo();
        g1.setGeo(gp1);
        g1.setPlace("1");
        g1.setUserId(1L);
        g1.setUserName("1");
        GeoDemo g2 = new GeoDemo();
        g2.setGeo(gp2);
        g2.setPlace("2");
        g2.setUserId(2L);
        g1.setUserName("2");
        GeoDemo g3 = new GeoDemo();
        g3.setGeo(gp3);
        g3.setPlace("3");
        g3.setUserId(3L);
        g3.setUserName("3");
        elasticsearchTemplate.save(Arrays.asList(g1,g2,g3));
        //save(g3,null);
        Thread.sleep(1500);
        //BoundingBoxQuery
        GeoPoint topLeft = new GeoPoint(32.030249,118.789703);
        GeoPoint bottomRight = new GeoPoint(32.024341,118.802171);
        GeoBoundingBoxQueryBuilder geoBoundingBoxQueryBuilder =
                QueryBuilders.geoBoundingBoxQuery("geo")
                .setCorners(topLeft,bottomRight);
        List<GeoDemo> search = elasticsearchTemplate.search(geoBoundingBoxQueryBuilder, GeoDemo.class);
    }
}
