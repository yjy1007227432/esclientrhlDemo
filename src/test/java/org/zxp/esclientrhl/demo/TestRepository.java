package org.zxp.esclientrhl.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.demo.repository.IndexDemoRepository;
import org.zxp.esclientrhl.enums.AggsType;
import java.util.Map;

/**
 * @program: Repository的使用
 * @description:
 * @author: X-Pacific zhang
 * @create: 2019-09-03 13:41
 **/
public class TestRepository extends EsclientrhlDemoApplicationTests {


    @Autowired
    IndexDemoRepository indexDemoRepository;

    /**
     * 保存
     * @throws Exception
     */
    @Test
    public void testSave() throws Exception {
        IndexDemo main2 = new IndexDemo();
        main2.setProposal_no("qq123549440");
        main2.setBusiness_nature_name("渠道");
        main2.setAppli_name("esclientrhl");
        indexDemoRepository.save(main2);
    }

    /**
     * 删除
     * @throws Exception
     */
    @Test
    public void testDel() throws Exception {
        indexDemoRepository.deleteById("qq123549440");
    }

    /**
     * 聚合
     * @throws Exception
     */
    @Test
    public void testAggs() throws Exception {
        Map map2 = indexDemoRepository.aggs("proposal_no", AggsType.count,null,"appli_name");
        map2.forEach((o, o2) -> System.out.println(o + "=====" + o2));
    }
}
