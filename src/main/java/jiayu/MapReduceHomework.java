package jiayu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceHomework {

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String stopWordsPath = args[2];
        String task = args[3];

        Configuration conf = new Configuration();
        conf.set("stopwords.path", stopWordsPath);

        Job job = Job.getInstance(conf, task.equalsIgnoreCase("task1") ? "Stock Count" : "Word Count");
        job.setJarByClass(MapReduceHomework.class);

        if (task.equalsIgnoreCase("task1")) {
            // 设置 Mapper 和 Reducer
            job.setMapperClass(StockCountMapper.class);
            job.setCombinerClass(StockCountReducer.class); 
            job.setReducerClass(StockCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
        } else if (task.equalsIgnoreCase("task2")) {
            // 设置 Mapper 和 Reducer
            job.setMapperClass(WordCountMapper.class);
            job.setCombinerClass(WordCountReducer.class); 
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
        } else {
            System.err.println("Invalid task specified. Use 'task1' or 'task2'.");
            System.exit(-1);
        }
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

