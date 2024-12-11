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

//Questão 6 - Mostrar a soma das bilheteria dos filmes em que os atores são também os diretores
public class SomaBilheteriaAtorIgualDiretor {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();


        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = new Job(conf,"Ex6 - Soma das bilheterias por diretor e ator iguais");

        job.setJarByClass(SomaBilheteriaAtorIgualDiretor.class);

        job.setMapperClass(MapForBoxOffice.class);
        job.setReducerClass(ReduceForBoxOffice.class);
        job.setCombinerClass(CombinerForBoxOfficeActor.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BilheteriaAtorEDiretor.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

class MapForBoxOffice extends Mapper<LongWritable, Text, Text, BilheteriaAtorEDiretor>{
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String linha = value.toString();
        String[] colunas = linha.split(",");

        if (!linha.startsWith("Movie")) {
            if(!colunas[1].isEmpty() && !colunas[3].isEmpty() && !colunas[8].isEmpty()) {
                String diretor = colunas[1];
                String ator = colunas[3];
                float bilheteria = Float.parseFloat(colunas[8]);
                con.write(new Text(diretor), new BilheteriaAtorEDiretor(ator, bilheteria));
            }
        }
    }
}

class CombinerForBoxOfficeActor extends Reducer<Text, BilheteriaAtorEDiretor,Text, BilheteriaAtorEDiretor>{
    public void reduce(Text key, Iterable<BilheteriaAtorEDiretor> values, Context con) throws IOException, InterruptedException {
        for(BilheteriaAtorEDiretor b : values){
            if(key.toString().equals(b.getAtor())){
                con.write(key, b);
            }
        }
    }
}

class ReduceForBoxOffice extends Reducer<Text, BilheteriaAtorEDiretor,Text, FloatWritable>{
    public void reduce(Text key, Iterable<BilheteriaAtorEDiretor> values, Context con) throws IOException, InterruptedException {
        float somaBilheteria = 0.f;
        for(BilheteriaAtorEDiretor b : values) {
            if(key.toString().equals(b.getAtor())) {
                somaBilheteria += b.getBilheteria();
            }
        }

        con.write(key, new FloatWritable(somaBilheteria));
    }
}

