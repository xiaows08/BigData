package cn.xiaows.studyelasticsearch5.web;

import cn.xiaows.studyelasticsearch5.service.ElasticService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
public class DataController {

    @Autowired
    private ElasticService elasticService;

    @GetMapping("/")
    public String index() {
        return "this is index page ~";
    }

    @GetMapping("/read")
    public long read(String date, String name) {
        if (date == null) {
            date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        }
        return elasticService.readProbAgg(date, name);
    }
}
