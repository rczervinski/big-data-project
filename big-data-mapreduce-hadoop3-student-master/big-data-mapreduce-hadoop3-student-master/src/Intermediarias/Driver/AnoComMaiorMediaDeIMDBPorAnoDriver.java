package Intermediarias.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

import Intermediarias.Writable.*;

//Questão 7 - Calcular a média de IMDB por ano
public class AnoComMaiorMediaDeIMDBPorAnoDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance(conf, "AnoComMaiorMedia");
        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(AnoComMaiorMediaDeIMDBPorAnoDriver.class);
        job.setMapperClass(AnoComMaiorNotaIMDBMapper.class);
        job.setCombinerClass(AnoComMaiorNotaIMDBCombiner.class);
        job.setReducerClass(AnoComMaiorNotaIMDBReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AverageNotaIMBDPorAnoWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new AnoComMaiorMediaDeIMDBPorAnoDriver(), args);
        System.exit(result);
    }

    public static class AnoComMaiorNotaIMDBMapper extends Mapper<LongWritable, Text, IntWritable, AverageNotaIMBDPorAnoWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (!line.startsWith("Movie")) {
                String[] columns = line.split(",");
                if (columns.length > 15) {
                    if (!columns[14].isEmpty() && !columns[15].isEmpty()) {
                        int ano = Integer.parseInt(columns[14]);
                        float nota_imdb = Float.parseFloat(columns[15]);


                        context.write(new IntWritable(ano), new AverageNotaIMBDPorAnoWritable(1, nota_imdb));

                    }
                }
            }
        }
    }

        public static class AnoComMaiorNotaIMDBCombiner extends Reducer<IntWritable, AverageNotaIMBDPorAnoWritable, IntWritable, AverageNotaIMBDPorAnoWritable> {

            public void reduce(IntWritable key, Iterable<AverageNotaIMBDPorAnoWritable> values, Context context) throws IOException, InterruptedException {

                int contador = 0;
                float nota_imdb = 0.0f;

                for (AverageNotaIMBDPorAnoWritable notas : values) {
                    nota_imdb += notas.getNota_imdb();
                    contador += notas.getCount();
                }
                context.write(key, new AverageNotaIMBDPorAnoWritable(contador, nota_imdb));

            }
        }

        public static class AnoComMaiorNotaIMDBReducer extends Reducer<IntWritable, AverageNotaIMBDPorAnoWritable, IntWritable, FloatWritable> {

            public void reduce(IntWritable key, Iterable<AverageNotaIMBDPorAnoWritable> values, Context context) throws IOException, InterruptedException {

                int contador = 0;
                float nota_imdb = 0.0f;

                for (AverageNotaIMBDPorAnoWritable notas : values) {
                    nota_imdb += notas.getNota_imdb();
                    contador += notas.getCount();
                }
                float media = nota_imdb / contador;
                context.write(key, new FloatWritable(media));

            }
        }
    }
