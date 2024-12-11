package basic;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AgruparLucroPorGenero {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = new Job(conf, "Agrupar Lucro por Genero");

        job.setJarByClass(AgruparLucroPorGenero.class);
        job.setMapperClass(MapForAgruparLucroPorGenero.class);
        job.setReducerClass(ReduceForAgruparLucroPorGenero.class);


        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}


class MapForAgruparLucroPorGenero extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
        String line = value.toString();
        String[] fields = line.split(",");
        if(!line.startsWith("Movie")){
            if(!fields[6].isEmpty() && !fields[11].isEmpty()){
                String genre = fields[6];
                long earning = Long.parseLong(fields[11]);
                con.write(new Text(genre), new LongWritable(earning));
            }
        }


    }
}

class ReduceForAgruparLucroPorGenero extends Reducer<Text, LongWritable, Text, Text> {
    public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {
        long earningSum = 0;

        for(LongWritable val : values){
            earningSum = val.get();
        }
        String finalValue = String.valueOf(earningSum);
        con.write(key, new Text(finalValue));
    }
}