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

// 8 - Encontrar todas as  combinacoes de ator principal e diretor e seu lucro medio por filme
public class HighestEarningByActorDirector {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Actor Director Average Earnings");
        job.setJarByClass(HighestEarningByActorDirector.class);

        FileInputFormat.addInputPath(job, new Path(files[0]));
        FileOutputFormat.setOutputPath(job, new Path(files[1]));

        job.setMapperClass(MovieMapper.class);
        job.setCombinerClass(MovieCombiner.class);
        job.setReducerClass(MovieReducer.class);

        job.setMapOutputKeyClass(MovieCombination.class);
        job.setMapOutputValueClass(EarningStats.class);

        job.setOutputKeyClass(MovieCombination.class);
        job.setOutputValueClass(FloatWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, MovieCombination, EarningStats> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Movie")) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length < 12) {
                return;
            }

            String director = fields[1].trim();
            String actor1 = fields[3].trim();
            long earning = Long.parseLong(fields[11].trim());

            if (!director.isEmpty() && !actor1.isEmpty()) {
                MovieCombination combination = new MovieCombination(actor1, director);
                EarningStats stats = new EarningStats(earning, 1);
                context.write(combination, stats);
            }
        }
    }

    public static class MovieCombiner extends Reducer<MovieCombination, EarningStats, MovieCombination, EarningStats> {

        @Override
        public void reduce(MovieCombination key, Iterable<EarningStats> values, Context context)
                throws IOException, InterruptedException {
            long totalEarnings = 0;
            int totalMovies = 0;

            for (EarningStats val : values) {
                totalEarnings += val.getEarnings();
                totalMovies += val.getMovieCount();
            }

            EarningStats result = new EarningStats(totalEarnings, totalMovies);
            context.write(key, result);
        }
    }

    public static class MovieReducer extends Reducer<MovieCombination, EarningStats, MovieCombination, FloatWritable> {

        @Override
        public void reduce(MovieCombination key, Iterable<EarningStats> values, Context context)
                throws IOException, InterruptedException {
            long totalEarnings = 0;
            int totalMovies = 0;

            for (EarningStats val : values) {
                totalEarnings += val.getEarnings();
                totalMovies += val.getMovieCount();
            }

            if (totalMovies > 0) {
                FloatWritable result = new FloatWritable((float) totalEarnings / totalMovies);
                context.write(key, result);
            }
        }
    }
}



