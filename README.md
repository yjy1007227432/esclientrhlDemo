# esclientrhl使用说明

这个demo集成了[ESClientRHL](https://gitee.com/zxporz/ESClientRHL)，是个简单的springboot工程，只要拥有一个es服务和本demo就可以完成客户端与es的交互

单测目录中还包含各种esclientRHL的高级功能使用示例

## 快速入门

### 前置准备
本地部署一个es服务，端口默认9200，即访问http://localhost:9200可以访问

可以下载本例中的es版本[7.3.1](https://www.elastic.co/cn/downloads/past-releases/elasticsearch-7-3-1)


### 步骤一、pom添加maven依赖
```
<properties>
    <java.version>1.8</java.version>
    <elasticsearch.version>7.3.1</elasticsearch.version>
</properties>
<dependency>
    <groupId>cn.zxporz</groupId>
    <artifactId>esclientrhl</artifactId>
    <version>7.0.2</version>
</dependency>
```
### 步骤二、springboot启动类添加注解`EnableESTools`

```
@SpringBootApplication
@EnableESTools(basePackages={"org.zxp.esclientrhl.demo.repository"},entityPath = {"org.zxp.esclientrhl.demo.domain"})
public class EsclientrhlDemoApplication
```

### 配置`application.properties`
```
server.port=8888
elasticsearch.host=127.0.0.1:9200
```


### 步骤三、创建es索引对应的实体类
```
@ESMetaData(indexName = "index_demo",number_of_shards = 3,number_of_replicas = 0,printLog = true)
public class IndexDemo {
    @ESID
    private String proposal_no;
    @ESMapping(datatype = DataType.keyword_type)
    private String risk_code;
    @ESMapping(datatype = DataType.text_type)
    private String risk_name;
    @ESMapping(keyword = true)
    private String business_nature;
    @ESMapping(datatype = DataType.text_type)
    private String business_nature_name;
    private String appli_code;//可以用默认值，这样会有appli_code.keyword可以直接搜
    @ESMapping(suggest = true)
    private String appli_name;
    private String insured_code;
    @ESMapping(ngram = true)
    private String insured_name;
    @ESMapping(datatype = DataType.date_type)
    private Date operate_date;
    @ESMapping(datatype = DataType.text_type)
    private String operate_date_format;
    @ESMapping(datatype = DataType.date_type)
    private Date start_date;
    @ESMapping(datatype = DataType.date_type)
    private Date end_date;
    @ESMapping(datatype = DataType.double_type)
    private double sum_amount;
    @ESMapping(datatype = DataType.double_type)
    private double sum_premium;
    @ESMapping(datatype = DataType.keyword_type)
    private String com_code;
……
```

### 步骤四、创建Repository接口
```
public interface IndexDemoRepository extends ESCRepository<IndexDemo,String> {
}
```

### 步骤五、调用
```
@RestController
public class IndexDemoController {
    @Autowired
    private IndexDemoRepository indexDemoRepository;
    //http://127.0.0.1:8888/demo/add
    @GetMapping("/demo/add")
    public String add() throws Exception {
        IndexDemo indexDemo = new IndexDemo();
        indexDemo.setProposal_no("1");
        indexDemo.setAppli_name("a1");
        indexDemo.setRisk_code("aa1");
        indexDemo.setSum_premium(1);
        indexDemoRepository.save(indexDemo);
        return "新增成功";
    }
    //http://127.0.0.1:8888/demo/add_list
    @GetMapping("/demo/add_list")
    public String addList() throws Exception {
        IndexDemo indexDemo2 = new IndexDemo();
        indexDemo2.setProposal_no("2");
        indexDemo2.setAppli_name("a2");
        indexDemo2.setRisk_code("aa2");
        indexDemo2.setSum_premium(2);

        IndexDemo indexDemo3 = new IndexDemo();
        indexDemo3.setProposal_no("3");
        indexDemo3.setAppli_name("a3");
        indexDemo3.setRisk_code("aa3");
        indexDemo3.setSum_premium(3);
        indexDemoRepository.save(Arrays.asList(indexDemo2,indexDemo3));
        return "新增成功";
    }
    //http://127.0.0.1:8888/demo/update
    @GetMapping("/demo/update")
    public String update() throws Exception {
        IndexDemo indexDemo = new IndexDemo();
        indexDemo.setProposal_no("1");
        indexDemo.setAppli_name("a999999");
        indexDemo.setRisk_code("aa9999999");
        indexDemo.setSum_premium(99999);
        indexDemoRepository.update(indexDemo);
        return "修改成功";
    }

    //http://127.0.0.1:8888/demo/delete
    @GetMapping("/demo/delete")
    public String delete() throws Exception {
        IndexDemo indexDemo = new IndexDemo();
        indexDemo.setProposal_no("1");
        indexDemo.setAppli_name("a999999");
        indexDemo.setRisk_code("a9999999");
        indexDemo.setSum_premium(99999);
        indexDemoRepository.delete(indexDemo);
        return "删除成功";
    }
    //http://127.0.0.1:8888/demo/query
    @GetMapping("/demo/query")
    public List<IndexDemo> query() throws Exception {
        List<IndexDemo> search = indexDemoRepository.search(QueryBuilders.matchAllQuery());
        return search;
    }
}
```

###  测试访问
1. 访问http://127.0.0.1:8888/demo/add，增加一条数据
1. 访问http://127.0.0.1:8888/demo/add_list，增加多条数据
1. 访问http://127.0.0.1:8888/demo/query，查询列表
1. 访问http://127.0.0.1:8888/demo/update，修改一条数据
1. 访问http://127.0.0.1:8888/demo/query，查询列表
1. 访问http://127.0.0.1:8888/demo/delete，删除一条数据
1. 访问http://127.0.0.1:8888/demo/query，查询列表


## 更多使用demo见`test`包下代码

1. TestCRUD 普通增删改查
1. TestAggs 聚合的使用
1. TestIndex 索引管理
1. TestLowLevelClient LowLevelClient的使用
1. TestNewClient 直接使用RestHighLevelClient操作
1. TestNested 嵌套对象的使用
1. TestQueryBuilder 各种QueryBuilder的使用
1. TestRepository Repository的使用
1. TestAliases 别名的使用
1. TestRollover Rollover的使用
1. TestSugg 搜索建议的使用
1. TestGeo GEO的使用

