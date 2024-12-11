package Basicas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

//Questão 3 - Agrupar o lucro por ator principal (Ator 1)
public class LucroPorAtorPrincipal {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(args[0]);

        Path output = new Path(args[1]);

        Job j = new Job(c, "Lucro por ator principal");

        // registro das classes
        j.setJarByClass(LucroPorAtorPrincipal.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(LongWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String line = value.toString();
            if(!line.startsWith("Movie")){
                String[] columns = line.split(",");
                if(columns.length > 13){
                    if(!columns[3].isEmpty() && !columns[11].isEmpty()){
                        String ator1 = columns[3];
                        long lucro = Long.parseLong(columns[11]);

                        con.write(new Text(ator1), new LongWritable(lucro));
                    }
                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            long conta = 0;
            for (LongWritable valor : values){
                conta = conta + valor.get();
            }
            con.write(key, new LongWritable(conta));
        }
    }
}
