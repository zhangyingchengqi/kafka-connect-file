package com.yc.kafkaConnect.connect4;

import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 输入连接器任务类: 这个任务类的主要功能是从一个文件中读取数据
 * 
 * @author 张影 QQ:1069595532 WX:zycqzrx1
 * @date Mar 19, 2020
 */
public class ZyFileStreamSourceTask extends SourceTask {

	// 声明一个日志类
	private static final Logger LOG = LoggerFactory.getLogger(ZyFileStreamSourceTask.class);
	// 定义文件字段
	public static final String FILENAME_FIELD = "filename";
	// 定义偏移量字段
	public static final String POSITION_FIELD = "position";
	// 定义值的值的数据格式
	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

	// 声明文件名
	private String filename;
	// 声明输入流对象
	private InputStream stream;
	// 声明读取对象
	private BufferedReader reader = null;   //带缓冲的字符流   它包装  stream
	// 定义缓冲区大小
	private char[] buffer = new char[1024];
	// 声明偏移量变量
	private int offset = 0;
	// 声明主题名
	private String topic = null;

	// 声明输入流偏移量
	private Long streamOffset;

	public ZyFileStreamSourceTask() {
		System.out.println("ZyFileStreamSourceTask构造方法");
	}

	/** 获取版本. */
	public String version() {
		String version = new ZyFileStreamSourceConnector().version();
		System.out.println("调用了 ZyFileStreamSourceTask 的 version() " + version);
		return version;
	}

	/** 开始执行任务.
	 * curl -i -X POST -H "Content-type:application/json" -H "Accept:application/json" 
	 * -d '{"name":"test-file-source1",
	 * "config":{"connector.class":"FileStreamSource","tasks.max":"1","topic":"connect-file-test","file":"foo.txt"}}'  http://localhost:8083/connectors
	 *  
	 *  */
	public void start(Map<String, String> props) {
		System.out.println("调用了 ZyFileStreamSourceTask 的 start() " + props);
		
		filename = props.get(ZyFileStreamSourceConnector.FILE_CONFIG);   // foo.txt
		// 如果不指定文件名，则从标准输入流中获取数据
		if (filename == null || filename.isEmpty()) {
			stream = System.in;   //标准输入流
			streamOffset = null;   //从控制台输入的话，就不需要记录这个源数据的偏移量，因为每一行操作一次. 
			reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		}
		// 获取主题名
		topic = props.get(ZyFileStreamSourceConnector.TOPIC_CONFIG);   // connect-file-test
		if (topic == null)
			throw new ConnectException("FileStreamSourceTask config missing topic setting");
	}

	/** 读取记录并返回数据集. */
	public List<SourceRecord> poll() throws InterruptedException {
		// 如果stream是空，说明是从一个文件中读取数据
		if (stream == null) {
			try {
				stream = new FileInputStream(filename);  //文件流
				// 注意这里的context是父类中的 SourceTaskContext,表示任务的上下文,理解成一个缓存    Map< < filename, foo.txt>, offset >
				// 在这里是要读取文件当前的偏移值            < < filename, foo.txt>, offset >
				Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
				// 如果offset不为空，说明以前读过数据，应该按保存的offset来偏移后再读取，否则从头读起
				if (offset != null) {
					// 获取这个文件的偏移量
					Object lastRecordedOffset = offset.get(POSITION_FIELD);   // 2
					if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long)) // 容错处理，位移必须是long
						throw new ConnectException("Offset position is the incorrect type");
					if (lastRecordedOffset != null) {  // 2
						// 存有偏移量，则将stream流定位到这个位置后， 从这个位置开始读
						LOG.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
						long skipLeft = (Long) lastRecordedOffset;
						// 循环位移来在源文件中定位下次要读取的数据的起始,
						while (skipLeft > 0) {  //  skipLeft: 2    >0
							try {
								//操作文件指针.
								long skipped = stream.skip(skipLeft); // 输入流的指针向前尝试移动    skip(  2 );
																		// skipLeft的位置,
																		// 返回值为实际移动的偏移量
								skipLeft -= skipped; // 计算按这个 skipped
														// 位移后还要走多少个字节
							} catch (IOException e) {
								LOG.error("Error while trying to seek to previous offset in file: ", e);
								throw new ConnectException(e);
							}
						}
						LOG.debug("Skipped to offset {}", lastRecordedOffset);
					}
					// 定位完成，当前位移就是要读取的位置
					streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
				} else { // 从头读起
					streamOffset = 0L;
				}
				//以上完成了两个功能:  1. 指向文件的指针位置     2. 本次要读取的文件的起始位数值
				
				// 创建流 ，准备完成读取
				reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
				LOG.debug("Opened {} for reading", logFilename());
			} catch (FileNotFoundException e) {
				LOG.warn("Couldn't find file {} for FileStreamSourceTask, sleeping to wait for it to be created", logFilename());
				//假如   foo.txt源文件被锁定，或读不到. 
				synchronized (this) {  //锁
					this.wait(1000);   //等待 1000,     释放锁  
				}
				return null;
			}
		}
		//   计算位置，   流的指针

		try {
			// 创建一个 BufferedReader完成读取， 注意加锁
			final BufferedReader readerCopy;
			synchronized (this) {
				readerCopy = reader;
			}
			if (readerCopy == null)
				return null;

			ArrayList<SourceRecord> records = null;

			int nread = 0;   //计数器: 记录到底读了多少个字节
			while (readerCopy.ready()) {
				// 从文件中的 offset位置读取 buffer.length-offset长度的数据存到 buffer中,
				// 返回实际读取的长度 nread 
				nread = readerCopy.read(buffer, offset, buffer.length - offset);       //如果读到数据了，这里  nread=-1    [a,b,'\n',a,b]
				LOG.trace("Read {} bytes from {}", nread, logFilename());
				// 如果nread>0说明读取数据了.计算新的位移
				if (nread > 0) {
					offset += nread; // 指针移动   offset=2    nread=3    offset=5
					// 注意，如果这次读取已经将缓冲区存满了， 则将原缓冲区扩容，扩容两倍的大小,将原缓冲区的数据复制进去
					if (offset == buffer.length) {
						char[] newbuf = new char[buffer.length * 2];
						System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
						buffer = newbuf;
					}
					//以上一次性读取最多    buffer.length-offset个字符
					String line;
					do {
						//这这么多个字符中找出换行符，当成一条消息来处理
						line = extractLine(); // 将此时 buffer中的数据 解析出来(注意是按行解析，一次只解析一行数据)
						if (line != null) {
							LOG.trace("Read a line from {}", logFilename());
							if (records == null)
								records = new ArrayList<>();
							// 将解析出来的数据以SourceRecord对象保存后保存到 list中                   										值类型        值     时间cuo
							SourceRecord sr = new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic, null, null, null, VALUE_SCHEMA, line, System.currentTimeMillis());
							records.add(sr);
						}
					} while (line != null); // line不为空，则继续解析
				}
			}   //解析出读取数据的每一行，馐成一个  SourceRecord   ，由kafka connect传到 topic

			if (nread <= 0) { // 如果nread小于0，说明输入文件中没有新内容了. 当前线程进入
								// wait状态，让出cpu控制权.
				synchronized (this) {
					this.wait(1000);
				}
			}

			return records;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/** 解析一条记录. */
	private String extractLine() {
		int until = -1;  //当前这一行的最后一个字节
		int newStart = -1;   //下一行的第一个字节
		// 最多读取  offset 个字节数据，从中找到 每一行的 结束符（有三种情况:  \n,  \r,和\r\n) ,其中  \n,\r\n都表示一行   如只有 \r则只有回车，这一行数据都会被覆盖.
		//  [a,b,'\n',a,b]
		for (int i = 0; i < offset; i++) {
			if (buffer[i] == '\n') {   //保证一行一行地读
				until = i;
				newStart = i + 1;
				break;
			} else if (buffer[i] == '\r') {   //有时候回车和换行是一起用的  \r\n
				if (i + 1 >= offset){
					return null;  //如果只有一个 \r回车的话，这一行的内容都会被清空，所以这一行内容为null
				}
				until = i;
				//如果  \r后面是 \n的话，新的开始位置为  i+2
				newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
				break;
			}
		}
		//到这里，已经找到一行数据的位置了    

		//下面，先判断 until是否为  -1, 如为-1 ，则表示已经没有文本数据了. 
		if (until != -1) {
			//创建   一个字符串，因为是文本文件,将  buffer 转为字符串
			String result = new String(buffer, 0, until);   //   -> 
			//将下一行的内容覆盖现有的  buffer.
			System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
			//偏移量也要减去已经读取到的数据长度
			offset = offset - newStart;
			if (streamOffset != null)
				streamOffset += newStart;   //流的指针加上 newStart,表示指针要向前移动 newStart个字节，下次从这个位置读取
			return result;
		} else {
			return null;
		}
	}

	/** 停止任务. */
	public void stop() {
		LOG.trace("Stopping");
		synchronized (this) {
			try {
				if (stream != null && stream != System.in) {
					stream.close();
					LOG.trace("Closed input stream");
				}
			} catch (IOException e) {
				LOG.error("Failed to close FileStreamSourceTask stream: ", e);
			}
			this.notify();  //通知其它的线程从 wait状态中唤醒. 
		}
	}

	private Map<String, String> offsetKey(String filename) {
		return Collections.singletonMap(FILENAME_FIELD, filename);
	}

	private Map<String, Long> offsetValue(Long pos) {
		return Collections.singletonMap(POSITION_FIELD, pos);
	}

	/** 判断是标准输入还是读取文件. */
	private String logFilename() {
		return filename == null ? "stdin" : filename;
	}

}
