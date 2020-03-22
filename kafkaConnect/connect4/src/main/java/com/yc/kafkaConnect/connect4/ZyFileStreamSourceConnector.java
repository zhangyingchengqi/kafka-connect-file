package com.yc.kafkaConnect.connect4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * 输入联接器: 用于读取配置信息和分配任务的一些初始化工作
 * @author 张影 QQ:1069595532 WX:zycqzrx1
 * @date Mar 19, 2020
 */
public class ZyFileStreamSourceConnector extends SourceConnector {

	// 定义主题配置变量
	public static final String TOPIC_CONFIG = "topic";    ///  
	// 定义文件配置变量
	public static final String FILE_CONFIG = "file";   

	//a.properties
	// # source filename
	// file＝
	// # The topic to publish data to
	// topic=
	
	// 实例化一个配置对象
	private static final ConfigDef CONFIG_DEF = new ConfigDef().define(FILE_CONFIG, Type.STRING, Importance.HIGH, "Source filename.").define(TOPIC_CONFIG, Type.STRING, Importance.HIGH,
			"The topic to publish data to");

	// 声明文件名变量
	private String filename;    //这是要读取的配置信息,从    curl命令的参数中读
	// 声明主题变量
	private String topic;
	
	public ZyFileStreamSourceConnector(){
		System.out.println(  "ZyFileStreamSourceConnector构造方法");
	}
	

	/** 获取版本. */
	public String version() {
		String version=AppInfoParser.getVersion();
		System.out.println("调用了 ZyFileStreamSourceConnector 的 version() "+version);
		return version;
	}

	/** 开始初始化.   读取配置信息    curl ->   http请求  ->  connect服务  ->   解析请求包装 成  props        */
	/*
	 *curl -i -X POST -H "Content-type:application/json" -H "Accept:application/json" 
	 *-d '{"name":"test-file-source1",
	 *"config":{"connector.class":"FileStreamSource","tasks.max":"1","topic":"connect-file-test","file":"foo.txt"}}'  
	 *
	 *http://localhost:8083/connectors
	 */
	public void start(Map<String, String> props) {
		System.out.println("调用了 ZyFileStreamSourceConnector 的 start() "+props);
		filename = props.get(FILE_CONFIG);  //读取配置项  file,获取文件名  =  props.get("file");  ->  foo.txt
		topic = props.get(TOPIC_CONFIG);   //读取配置项  topic,获取主题名  =  props.get("topic");  -> connect-file-test
		if (topic == null || topic.isEmpty())  //如果没有指定主题，则异常
			throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
		if (topic.contains(","))  //只允许一个connect同时从一个文件中读取数据
			throw new ConnectException("FileStreamSourceConnector should only have a single topic when used as a source.");
	}

	/** 实例化输入任务类. */
	public Class<? extends Task> taskClass() {
		System.out.println("调用了 ZyFileStreamSourceConnector 的 taskClass() ");
		return ZyFileStreamSourceTask.class;  //  返回任务对象的反射  ->       ZyFileStreamSourceTask.class.newInstance();  -》无参构造方法
	}

	/** 根据配置的最大任务数，创建对应的配置信息.其中主题一定要有 
	 *     在这里，多个任务共用一个配置文件.
	 * */
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		System.out.println("调用了 ZyFileStreamSourceConnector 的 taskConfigs() "+maxTasks);
		
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		
		Map<String, String> config = new HashMap<>();
		if (filename != null)
			config.put(FILE_CONFIG, filename);
		config.put(TOPIC_CONFIG, topic);
		configs.add(config);
		return configs;
	}

	@Override
	public void stop() {
		System.out.println("关闭 ZyFileStreamSourceConnector ");
	}

	/** 获取配置对象. */
	public ConfigDef config() {
		return CONFIG_DEF;
	}

}
