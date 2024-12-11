package Intermediarias.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import Intermediarias.Writable.*;

// Questão 5 - Calcular a média das notas do IMDb por gênero
public class AvgImdbByGender {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();


        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = new Job(conf,"Avg Imdb by Gender");

        job.setJarByClass(AvgForIMDBWritable.class);
        job.setMapperClass(MapForAvgIMDB.class);
        job.setReducerClass(ReduceForAvgIMDB.class);
        job.setCombinerClass(CombinerForAVGIMDB.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgForIMDBWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}


class MapForAvgIMDB extends Mapper<LongWritable, Text, Text, AvgForIMDBWritable>{
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        if (!line.startsWith("Movie")) {
            if(!fields[6].isEmpty() && !fields[15].isEmpty()) {
                String genre = fields[6];
                float imdb = Float.parseFloat(fields[15]);

                con.write(new Text(genre), new AvgForIMDBWritable(imdb,1));
            }

        }
    }

}

class CombinerForAVGIMDB extends Reducer<Text, AvgForIMDBWritable,Text, AvgForIMDBWritable>{
    public void reduce(Text key, Iterable<AvgForIMDBWritable> values, Context con) throws IOException, InterruptedException {
        float sum_imdb = 0f;
        int sum = 0;

        for(AvgForIMDBWritable val : values){
            sum_imdb += val.getImdb();
            sum += val.getSum();
        }
        AvgForIMDBWritable avg = new AvgForIMDBWritable(sum_imdb, sum);
        con.write(key, avg);
    }
}

class ReduceForAvgIMDB extends Reducer<Text, AvgForIMDBWritable,Text, FloatWritable>{
    public void reduce(Text key, Iterable<AvgForIMDBWritable> values, Context con) throws IOException, InterruptedException {
        float sum_imdb = 0f;
        int sum = 0;

        for(AvgForIMDBWritable val : values){
            sum_imdb += val.getImdb();
            sum += val.getSum();
        }
        Float avg_imdb = sum_imdb / sum;
        con.write(key, new FloatWritable(avg_imdb));
    }
}
