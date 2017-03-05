package cn.itcast.hadoop.mr.flowSum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

	
	//1.框架每传递一组数据<1387788654,{FlowBean,FlowBean,...}> 调用一次我们的reduce方法
	//reduce中的业务逻辑就是遍历 values，然后进行累加求和再输出
	@Override
	protected void reduce(Text key1, Iterable<FlowBean> values1, Context context)
			throws IOException, InterruptedException {
		long up_flow_counter=0;
		long down_flow_conter=0;
		
		//2.将map提供的上行流量和下行流量进行累加
		for(FlowBean bean:values1)
		{
			up_flow_counter+=bean.getUp_flow();
			down_flow_conter+=bean.getDown_flow();
		}
		
		//3.然后将，电话号码 key1和总的上行和下行流量封装到一个FlowBean对象当中，发送出去
		//4.但是有一个问题，reduce在将数据写入文本的时候是调用变量的tostring方法，但是reduce怎么知道要怎样把你的flowbean写到文本当中呢
		//因此你需要在FlowBean这个类当中，重写ToString 这个方法。写完之后就可以愉快地进行context传输了。
		context.write(key1, new FlowBean(key1.toString(),up_flow_counter,down_flow_conter));
	}
	
	
	
	
}
