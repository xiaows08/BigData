package com.xiaows.test;

import com.xiaows.dao.ESUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * @Description:
 * @Auther: XIAOWS
 * @Date: 2018/7/26 15:05
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:spring-context.xml"})
public class ESConnTest {

	@Resource
	private ESUtils esConn;

	@Test
	public void test1() {
		esConn.test();
	}
}