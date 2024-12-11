package Avancadas.Drivers;

import Avancadas.Writable.DiretorComMediaDeBilheteria;
import Avancadas.Writable.InfluenciaDiretorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

//10 - Calcular o lucro total e a quantidade de prêmios do Oscar
// recebidos para filmes dirigidos por diretores com uma média de bilheteria por filme superior a 50%.
public class LucroTotalPorDiretorInfluente extends Configured implements Tool {

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path intermediate = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job1 = Job.getInstance(conf);
        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);

        job1.setJarByClass(LucroTotalPorDiretorInfluente.class);
        job1.setMapperClass(DiretorBilheteriaMap.class);
        job1.setReducerClass(DiretorBilheteriaReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DiretorComMediaDeBilheteria.class);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf);

            FileInputFormat.addInputPath(job2, intermediate);
            FileSystem.get(conf).delete(output, true);
            FileOutputFormat.setOutputPath(job2, output);

            job2.setJarByClass(LucroTotalPorDiretorInfluente.class);
            job2.setMapperClass(OscarMapper.class);
            job2.setReducerClass(OscarReducer.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(InfluenciaDiretorWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            return job2.waitForCompletion(true) ? 0 : 1;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new LucroTotalPorDiretorInfluente(), args);
        System.exit(result);
    }

    public static class DiretorBilheteriaMap extends Mapper<LongWritable, Text, Text, DiretorComMediaDeBilheteria> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] colunas = linha.split(",");

            if (!linha.startsWith("Movie")) {
                if (!colunas[1].isEmpty() && !colunas[10].isEmpty() && !colunas[13].isEmpty() && !colunas[11].isEmpty()) {
                    String diretor = colunas[1];
                    Double porcentagemBilheteria = Double.parseDouble(colunas[10]);
                    Double oscar = Double.parseDouble(colunas[13]);
                    Double lucro = Double.parseDouble(colunas[11]);

                    context.write(new Text(diretor), new DiretorComMediaDeBilheteria(porcentagemBilheteria, 1, lucro, oscar));
                }
            }
        }
    }

    public static class DiretorBilheteriaReducer extends Reducer<Text, DiretorComMediaDeBilheteria, Text, Text> {
        public void reduce(Text key, Iterable<DiretorComMediaDeBilheteria> values, Context con) throws IOException, InterruptedException {
            Double conta_bilheteria = 0.0;
            Double lucro_total = 0.0;
            Double oscar_total = 0.0;
            int quantidade_filmes = 0;

            for (DiretorComMediaDeBilheteria d : values) {
                conta_bilheteria += d.getPorcentagemBilheteria();
                lucro_total += d.getLucro();
                oscar_total += d.getOscar();
                quantidade_filmes += d.getSum();
            }

            DiretorComMediaDeBilheteria resultado = new DiretorComMediaDeBilheteria(conta_bilheteria, quantidade_filmes, lucro_total, oscar_total);

            con.write(key, new Text(resultado.toString()));
        }
    }

    public static class OscarMapper extends Mapper<LongWritable, Text, Text, InfluenciaDiretorWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] colunas = linha.split("\t"); //separa a chave dos valores, vem com um tab

            // Separa agora os valores certinho
            String[] valores = colunas[1].trim().split(" ");

            if (valores.length >= 4) {
                String diretor = colunas[0];
                Double mediaBilheteria = Double.parseDouble(valores[0].replace(",", "."));
                int quantidadeFilmes = Integer.parseInt(valores[1]);
                Double lucroTotal = Double.parseDouble(valores[2].replace(",", "."));
                Double oscarTotal = Double.parseDouble(valores[3].replace(",", "."));

                if (mediaBilheteria/quantidadeFilmes > 50) {
                    context.write(
                            new Text(diretor),
                            new InfluenciaDiretorWritable(lucroTotal, oscarTotal, quantidadeFilmes)
                    );
                }
            }
        }
    }

    public static class OscarReducer extends Reducer<Text, InfluenciaDiretorWritable, Text, Text> {
        public void reduce(Text key, Iterable<InfluenciaDiretorWritable> values, Context con) throws IOException, InterruptedException {

            Double oscar = 0.0;
            Double ganho = 0.0;
            int filmes = 0;

            for (InfluenciaDiretorWritable i : values) {
                oscar += i.getMedia_oscar();
                ganho += i.getMedia_ganhos();
                filmes += i.getQuantidade_filmes();
            }

            if (filmes > 0) {
                Double media_oscar = oscar / filmes;
                Double media_ganho = ganho / filmes;

                InfluenciaDiretorWritable resultado = new InfluenciaDiretorWritable(media_ganho, media_oscar, filmes);
                con.write(key, new Text(resultado.toString()));
            }
        }
    }
}
