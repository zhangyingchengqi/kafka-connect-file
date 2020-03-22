package com.yc.kafkaConnect.connect4;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  输出连接器任务类: 这个任务类的主要功能是将数据从topic中取出保存到一个文件
 * @author 张影 QQ:1069595532 WX:zycqzrx1
 * @date Mar 19, 2020
 */
public class ZyFileStreamSinkTask extends SinkTask{
	// 声明一个日志对象
	private static final Logger LOG = LoggerFactory.getLogger(ZyFileStreamSinkTask.class);

	// 声明一个文件名变量
	private String filename;
	// 声明一个输出流对象
	private PrintStream outputStream;

	/** 构造函数. */
	public ZyFileStreamSinkTask() {
		System.out.println("ZyFileStreamSinkTask构造方法");
	}

	/** 重载构造函数. */
	public ZyFileStreamSinkTask(PrintStream outputStream) {
		System.out.println("ZyFileStreamSinkTask构造方法2");
		filename = null;
		this.outputStream = outputStream;
	}

	/** 获取版本号. */
	public String version() {
		String version = new ZyFileStreamSinkConnector().version();
		System.out.println("调用了 ZyFileStreamSinkTask 的 version() " + version);
		return version;
	}

	/** 开始执行任务. */
	public void start(Map<String, String> props) {
		System.out.println("调用了 ZyFileStreamSinkTask 的 start() " + props);
		filename = props.get(ZyFileStreamSinkConnector.FILE_CONFIG);
		if (filename == null) {
			outputStream = System.out;
		} else {
			try {
				outputStream = new PrintStream(new FileOutputStream(filename, true), false, StandardCharsets.UTF_8.name());
			} catch (FileNotFoundException | UnsupportedEncodingException e) {
				throw new ConnectException("Couldn't find or create file for FileStreamSinkTask", e);
			}
		}
	}

	/** 发送记录给Sink并输出. */
	public void put(Collection<SinkRecord> sinkRecords) {
		System.out.println("调用了 ZyFileStreamSinkTask 的 put() " );
		for (SinkRecord record : sinkRecords) {
			LOG.trace("Writing line to {}: {}", logFilename(), record.value());
			outputStream.println(record.value());
		}
	}

	/** 持久化数据. */
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		System.out.println("调用了 ZyFileStreamSinkTask 的 flush() " );
		LOG.trace("Flushing output stream for {}", logFilename());
		outputStream.flush();
	}

	/** 停止任务. */
	public void stop() {
		System.out.println("调用了 ZyFileStreamSinkTask 的 stop() " );
		if (outputStream != null && outputStream != System.out)
			outputStream.close();
	}

	/** 判断是标准输出还是文件写入. */
	private String logFilename() {
		return filename == null ? "stdout" : filename;
	}
}
