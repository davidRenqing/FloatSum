package cn.itcast.hadoop.mr.flowSum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

//这个类继承map类。
//KEYIN :文本的偏移量；VALUEIN：Text；
//Map的输出，KEYOUT：Text（String），VALUEOUT：上行流量和下行流量组成的结构体，FlowBean
public class FlowSumMapper  extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	//拿到日志一行中的日志，切分各个字段。
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();                         //从文本当中拿到一行数据
		String [] fields=org.apache.commons.lang.StringUtils.split(line, "\t");   // 导包：org.apache.commons.lang.StringUtils.split(str, separatorChar)
		
		
		String phoneNB=fields[1];              //将电话号提取出来，字符从0开始
		Long up_flow=Long.parseLong(fields[7]);                //先将第8个字符提取出来，之后转换成long类型
		Long down_flow=Long.parseLong(fields[8]);              //拿到第9个字符串，再转换成long类型
		
		//将map切分的数据再输出出去
		//注意这个FlowBean对象，你将电话号码，上行流量，下行流量，传给这个新建的对象，因为你重写那两个函数嘛，所以当需要序列化的时候，会自动调用你重写的那两个 write和反序列化的函数的
		context.write(new Text(phoneNB), new FlowBean(phoneNB,up_flow,down_flow));  
	}
}
