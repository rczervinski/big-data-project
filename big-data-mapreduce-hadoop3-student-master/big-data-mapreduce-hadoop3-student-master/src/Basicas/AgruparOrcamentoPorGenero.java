package Basicas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


//Questão 4 - Agrupar o orçamento de todos os filmes por gênero
public class AgruparOrcamentoPorGenero {
    public static void main(String[] args) throws IOException, Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = new Job(conf, "AgruparOrcamentoPorGenero");

        job.setJarByClass(AgruparOrcamentoPorGenero.class);
        job.setMapperClass(AgruparOrcamentoPorGeneroMapper.class);
        job.setReducerClass(AgruparOrcamentoPorGeneroReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

        public static class AgruparOrcamentoPorGeneroMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] columns = line.split(",");

                if(!line.startsWith("Movie")){
                    if(!columns[6].isEmpty() && !columns[7].isEmpty()){
                        String genero = columns[6];
                        long orcamento = Long.parseLong(columns[7]);
                        context.write(new Text(genero), new LongWritable(orcamento));
                    }
                }
            }
        }
        public static class AgruparOrcamentoPorGeneroReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
            public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                long somaOrcamento = 0;

                for (LongWritable val : values) {
                    somaOrcamento += val.get();
                }

                context.write(key, new LongWritable(somaOrcamento));

            }
        }

    }

