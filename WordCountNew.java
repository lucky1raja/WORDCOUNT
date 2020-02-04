
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountNew {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
	// 1111L , hadoop is one of the bigdata tool---> hadoop , 1  

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // To Write the Mapper Business Logic  , we need to override below
    // map method
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());//hadoop , is //hadoop is bigdata tool  // ---- , 
                                                             // hadoop
        System.out.println("Word Is:" + word);                                                     // is
                                                             // bigdata
                                                             // tool
        context.write(word, one); // hadoop , 1  is , 1----tool,1
        
      }      
    }
  }// End Of Map Method

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  // hadoop , 1,1,1,1---> hadoop	4
    private IntWritable result = new IntWritable();
    // To Write the Reducer Business Logic  , we need to override below
    // reduce method
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	// hadoop , 1,1,1,1
      int sum = 0; //4
            for (IntWritable val : values) { // hadoop,,,,
        sum += val.get(); // sum = sum+val.get();
                          //  3 = 3 + 1;
        
      }
     System.out.println("Sum is--->" + sum);      
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	System.out.println("*****In the Driver Code*****");
	//Configuration Details w.r.to JOB,JAR etc
    Configuration conf = new Configuration();
    Job job = new Job(conf, "WORD COUNT JOB");
    job.setJarByClass(WordCountNew.class);
    
    // Mapper , Combiner & Reducer Class Name details
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
       
    // Final Output KEY , VALUE Datatype details
    
    job.setOutputKeyClass(Text.class); //hadoop
    job.setOutputValueClass(IntWritable.class); //4
    
    // HDFS Input Path , HDFS Output Path Details    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    //System Exit process
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}