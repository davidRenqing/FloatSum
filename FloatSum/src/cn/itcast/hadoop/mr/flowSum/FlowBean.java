package cn.itcast.hadoop.mr.flowSum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


//这个FlowBean 要实现序列化机制，要实现 Writable 接口
//点击FlowBean 就会提示实现未实现的方法 
//注意实现这个类之后，要提供一个FlowBean 作为泛型类的入口
public class FlowBean implements WritableComparable<FlowBean>{

	private String phoneNB;         //电话号码
	private long up_flow;           //上行流量
	private long down_flow;         //下行流量
	private long sum_flow;          //总的流量    
	
	//快速生成get和set的方法
	public String getPhoneNB() {
		return phoneNB;
	}
	public void setPhoneNB(String phoneNB) {
		this.phoneNB = phoneNB;
	}
	public long getUp_flow() {
		return up_flow;
	}
	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}
	public long getDown_flow() {
		return down_flow;
	}
	public void setDown_flow(long down_flow) {
		this.down_flow = down_flow;
	}
	public long getSum_flow() {
		return sum_flow;
	}
	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}

	//为了反序列化时，反序列化时需要使用无参数的构造函数
	public FlowBean(){}
	
	//这是使用 source那个选项，自动生成的构造函数
	//为了初始化数据方便，加入一个带参的构造函数
	public FlowBean(String phoneNB, long up_flow, long down_flow) {
		super();
		this.phoneNB = phoneNB;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.sum_flow = up_flow+down_flow;
	}
	//这个方法是当你读数据的时候调用这个函数
	//这个是反序列的方法
	@Override
	public void readFields(DataInput in) throws IOException {
		//等到反序列化你可以自己writeByString() 或者使用封装好的函数 
		phoneNB= in.readUTF();          //将传进来的文件流，按照string类型所占的字节，赋值给phoneNB
		up_flow=in.readLong();
		down_flow=in.readLong();
		sum_flow=in.readLong();
	}

	
	//这个方法是当你将数据发送到网络中的时候，将数字信息序列化到数据流中。等在网络中发送时，这些数据就是整体发送了
	@Override
	public void write(DataOutput out) throws IOException {
		//将我的数据写出去
		//等到将我的FolwBean对象写出去的时候，先将phonneNB转换成字节写出去，然后是up_flow,down_flow,sum_flow
		out.writeUTF(phoneNB);         //将我的 phoneNB 变量按照string类型所占的字节进行序列化
		out.writeLong(up_flow);        //将我的 up_flow 按照 long类型所占的字节数进行序列化
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
		
	}

	//重写toString 方法，使得reduce知道怎样把FlowBean这个对象写到文本当中
	@Override
	public String toString() {
		return ""+up_flow+"\t"+down_flow+"\t"+sum_flow;
	}
	
	//要将这个compareTo 这个方法进行重写
	//默认的是我比你传进来的参数大，返回+1；我比你传进来的参数小，返回-1；我给你传进来的参数相等，返回,0
	@Override
	public int compareTo(FlowBean o) {
		//现在是倒叙排序，我比你大，我就排到后面；因此我比你大我就返回-1
		return sum_flow>o.getSum_flow()?-1:1;
	}
}
