package cn.itcast.hadoop.mr.flowsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itcast.hadoop.mr.flowSum.FlowBean;
import cn.itcast.hadoop.mr.flowSum.FlowRunner;
import cn.itcast.hadoop.mr.flowSum.FlowSumMapper;
import cn.itcast.hadoop.mr.flowSum.FlowSumReducer;

public class SortMR {

	//将map和reducer全部用静态类来实现
	//1.这个程序是基于上一个程序实现的，定义KeyIn，ValueIn,KeyOut,ValueOut 的类型
	//nullWritable 是null 的序列化机制后的
	public static class SortMapper extends Mapper<LongWritable, Text,FlowBean, NullWritable>{
		
		//1.1重写map方法
		//拿到一行数据，切分出各个字段，封装为一个flowBean,作为key输出
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			 String line = value.toString();          //拿到一行
			 String[] fields = StringUtils.split(line, "\t");              //将你拿到的一行按照 "\t" 分开
			 String phoneNB=fields[0];                   //0个是电话号码
			 Long up_flow=Long.parseLong(fields[1]);                     //1个是上行流量
			 Long down_flow=Long.parseLong(fields[2]);                   //2是下行流量
			 
			 //将整个FlowBean对象作为key输出出去
			 //调用 NullWritable.get() 方法
			 context.write(new FlowBean(phoneNB, up_flow, down_flow), NullWritable.get());
		}
	}
	
	public static class SortReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable>{
		
		//重写reduce函数，没有其他的作用，就是将从map那里得到的结果在输出出去
		@Override
		protected void reduce(FlowBean key1, Iterable<NullWritable> values1,Context context)
				throws IOException, InterruptedException {
			context.write(key1, NullWritable.get());
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		//1.新建一个conf文件
				//导包：org.apache.hadoop.conf.Configuration
				Configuration conf=new Configuration();
				Job job=Job.getInstance(conf);          //2.创建一个job对象；导包：org.apache.hadoop.mapreduce.Job
				job.setJarByClass(SortMR.class);           //3.为这个job分配一个main类的包
				
				job.setMapperClass(SortMapper.class);       //4.设置job运行的map的类
				job.setReducerClass(SortReducer.class);       //设置job运行的reduce的类
				job.setMapOutputKeyClass(FlowBean.class);            //5.设置map的输出的key的类型
				job.setMapOutputValueClass(NullWritable.class);      //设置map输出的class
				
				job.setOutputKeyClass(FlowBean.class);              //6.设置reduce的输出的key的类
				job.setOutputValueClass(NullWritable.class);         //设置reduce的输出的value的类 
				
				//7.设置map阶段的输出路径和reduce的输出路径
				//可是使用 arg参数动态给传入
				//导包：org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K, V>
				//org.apache.hadoop.fs.Path.Path(String arg0)
				FileInputFormat.setInputPaths(job, new Path(args[0]));   
				
				//8.设置reduce的输出的结果存在在哪个目录
				//导包：org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<K, V>
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				//9.返回最终的结果
				//注意这个run函数的返回值是int类型的，而你的job返回值类型是bool类型的
				System.exit(job.waitForCompletion(true)?0:1);
	}
	 
}
