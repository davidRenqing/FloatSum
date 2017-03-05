package cn.itcast.hadoop.mr.flowSum;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//这是更新之后的版本的，Runner 函数的新的写法
//导包：import org.apache.hadoop.conf.Configured;；org.apache.hadoop.util.Tool
public class FlowRunner extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		//1.新建一个conf文件
		//导包：org.apache.hadoop.conf.Configuration
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);          //2.创建一个job对象；导包：org.apache.hadoop.mapreduce.Job
		job.setJarByClass(FlowRunner.class);           //3.为这个job分配一个main类的包
		
		job.setMapperClass(FlowSumMapper.class);       //4.设置job运行的map的类
		job.setReducerClass(FlowSumReducer.class);       //设置job运行的reduce的类
		job.setMapOutputKeyClass(Text.class);            //5.设置map的输出的key的类型
		job.setMapOutputValueClass(FlowBean.class);      //设置map输出的value的类型
		
		job.setOutputKeyClass(Text.class);              //6.设置reduce的输出的key的类
		job.setOutputValueClass(FlowBean.class);         //设置reduce的输出的value的类 
		
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
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		
		//导包：org.apache.hadoop.util.ToolRunner 
		//Tool:就是你的那个FlowRunner继承了Tool这个类，所以new一个FlowRunner类就可以了
		//return：返回值是整型；1代表正常执行返回，0代表非正常执行返回
		int res=ToolRunner.run(new Configuration(), new FlowRunner(), args);
		
		//2.指定系统退出的，正常执行完那个函数就正常退出；异常退出那个程序就异常退出程序
		System.exit(res);
	}
}
