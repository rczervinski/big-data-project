package Basicas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

//Questão 2 - Agrupar o número de prêmios do Oscar por gênero
public class OscarByGender {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "oscar by gender");

        job.setJarByClass(OscarByGender.class);
        job.setMapperClass(MapForOscarByGender.class);
        job.setReducerClass(ReduceForOscarByGender.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForOscarByGender extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (!line.startsWith("Movie")) {

                if (!fields[13].isEmpty() && !fields[6].isEmpty()) {
                    int awards = Integer.parseInt(fields[13].trim().replaceAll("\"", ""));
                    String movieGenres = fields[6];
                    con.write(new Text(movieGenres), new IntWritable(awards));

                }
            }
        }
    }

    public static class ReduceForOscarByGender extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int totalAwards = 0;
            for (IntWritable val : values) {
                totalAwards += val.get();
            }
            con.write(key, new IntWritable(totalAwards));
        }
    }
}