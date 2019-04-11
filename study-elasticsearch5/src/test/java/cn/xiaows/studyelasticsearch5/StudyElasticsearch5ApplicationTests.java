package cn.xiaows.studyelasticsearch5;

import cn.xiaows.studyelasticsearch5.service.ElasticService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class StudyElasticsearch5ApplicationTests {

    @Autowired
    private ElasticService elasticService;

    @Test
    public void read() {
        elasticService.readProbAgg("2018-08-27", "荣威");
    }
    @Test
    public void test11() {
        elasticService.test11("2018-08-27", "荣威");
    }

    @Test
    public void contextLoads() {
    }

}
