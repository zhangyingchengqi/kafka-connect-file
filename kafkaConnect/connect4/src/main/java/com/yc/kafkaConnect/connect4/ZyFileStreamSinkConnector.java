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
import org.apache.kafka.connect.sink.SinkConnector;

/**
* @author 张影 QQ:1069595532  WX:zycqzrx1
* @date Mar 19, 2020
*/
public class ZyFileStreamSinkConnector extends SinkConnector {

	// 声明文件配置变量
	public static final String FILE_CONFIG = "file";
	// 实例化一个配置对象   properties
   //    file=文件名
	private static final ConfigDef CONFIG_DEF = new ConfigDef().define(FILE_CONFIG, Type.STRING, Importance.HIGH, "Destination filename.");

	// 声明一个文件名变量
	private String filename;
	
	public ZyFileStreamSinkConnector(){
		System.out.println(  "ZyFileStreamSinkConnector构造方法");
	}

	/** 获取版本信息. */
	public String version() {
		String version=AppInfoParser.getVersion();
		System.out.println("调用了 ZyFileStreamSinkConnector 的 version() "+version);
		return version;
	}

	/** 执行初始化.
	 * curl -i -X POST -H "Content-type:application/json"  -d '{"name":"zy-distributed-console-sink","config":{"connector.class":"com.yc.kafkaConnect.connect4.ZyFileStreamSinkConnector","tasks.max":"1","topics":"distributed_connect_test",
	 * 
	 * 
	 * "file":"/tmp/bar.txt"}}'  http://localhost:8083/connectors
	 *  
	 *  */
	public void start(Map<String, String> props) {
		filename = props.get(FILE_CONFIG);
		System.out.println("调用了 ZyFileStreamSinkConnector 的 start() "+props);
	}

	/** 实例化输出类.*/
	public Class<? extends Task> taskClass() {
		System.out.println("调用了 ZyFileStreamSinkConnector 的 taskClass() ");
		return ZyFileStreamSinkTask.class;
	}

	/** 获取配置信息. */
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		System.out.println("调用了 ZyFileStreamSinkConnector 的 taskConfigs() "+maxTasks);
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> config = new HashMap<>();
			if (filename != null)
				config.put(FILE_CONFIG, filename);
			configs.add(config);
		}
		return configs;
	}

	public void stop() {
		System.out.println("关闭 ZyFileStreamSinkConnector ");
	}

	/** 获取配置对象. */
	public ConfigDef config() {
		return CONFIG_DEF;
	}

}

