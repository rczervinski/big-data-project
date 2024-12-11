package hard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DriverMainActorPerFilmByYear implements Tool {
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job1 = Job.getInstance(conf, "Highest Earning Movie Per Year");

        job1.setJarByClass(DriverMainActorPerFilmByYear.class);
        job1.setMapperClass(MapHighestEarningMovie.class);
        job1.setCombinerClass(CombineHighestEarningMovie.class);
        job1.setReducerClass(ReduceHighestEarningMovie.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(MovieWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(MovieWritable.class);

        Path input = new Path(args[0]);
        Path intermediate = new Path(args[1]);
        Path output = new Path(args[2]);

        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "Job2 Main actor by genre");

            FileInputFormat.addInputPath(job2, intermediate);
            FileSystem.get(conf).delete(output, true);
            FileOutputFormat.setOutputPath(job2, output);

            job2.setJarByClass(DriverMainActorPerFilmByYear.class);
            job2.setMapperClass(MapHighestBoxOffice.class);
            job2.setReducerClass(ReduceHighestBoxOffice.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(HighestBoxOfficeWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(HighestBoxOfficeWritable.class);
            return job2.waitForCompletion(true) ? 0 : 1;

        }


        return 1;
    }


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(),
                new DriverMainActorPerFilmByYear(), args);
        System.exit(result);
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }


    public static class MapHighestEarningMovie extends Mapper<LongWritable, Text, IntWritable, MovieWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
                String line = value.toString();

                if (line.startsWith("Movie")) {
                    return;
                }

                String[] fields = line.split(",");

                if (fields.length >= 15) {
                    String movie = fields[0].trim();
                    String director = fields[1].trim();
                    String actor1 = fields[3].trim();
                    String actor2 = fields[4].trim();
                    String actor3 = fields[5].trim();
                    String genre = fields[6].trim();

                    float percentActor = Float.parseFloat(fields[9].trim());
                    float percentDirector = Float.parseFloat(fields[10].trim());
                    Long earning = Long.parseLong(fields[11].trim());
                    int release = Integer.parseInt(fields[14].trim());

                    if (!movie.isEmpty() && !director.isEmpty() && earning > 0) {
                        MovieWritable movieWritable = new MovieWritable(
                                movie, earning, actor1, actor2, actor3,
                                director, genre, percentActor, percentDirector
                        );
                        con.write(new IntWritable(release), movieWritable);

                    }
                }
            }
    }


    public static class CombineHighestEarningMovie extends Reducer<IntWritable, MovieWritable, IntWritable, MovieWritable> {
        public void reduce(IntWritable key, Iterable<MovieWritable> values, Context con) throws IOException, InterruptedException {
            MovieWritable maxEarning = null;

            for (MovieWritable value : values) {
                if (maxEarning == null || value.getEarnings() > maxEarning.getEarnings()) {
                    maxEarning = new MovieWritable(value.getMovieName(), value.getEarnings(),
                            value.getActor1(), value.getActor2(), value.getActor3(),
                            value.getDirector(), value.getGenre(), value.getPercentActor(), value.getPercentDirector());
                }
            }
            if (maxEarning != null) {
                con.write(key, maxEarning);
            }
        }
    }


    public static class ReduceHighestEarningMovie extends Reducer<IntWritable, MovieWritable, IntWritable, MovieWritable> {
        public void reduce(IntWritable key, Iterable<MovieWritable> values, Context con) throws IOException, InterruptedException {
            MovieWritable maxEarning = null;

            for (MovieWritable value : values) {
                if (maxEarning == null || value.getEarnings() > maxEarning.getEarnings()) {
                    maxEarning = new MovieWritable(value.getMovieName(), value.getEarnings(),
                            value.getActor1(), value.getActor2(), value.getActor3(),
                            value.getDirector(), value.getGenre(), value.getPercentActor(), value.getPercentDirector());
                }
            }
            if (maxEarning != null) {
                con.write(key, maxEarning);
            }
        }
    }

    public static class MovieWritable implements Writable {
        private String movieName;
        private Long earnings;
        private String actor1;
        private String actor2;
        private String actor3;
        private String director;
        private String genre;
        private float percentActor;
        private float percentDirector;

        public MovieWritable() {
        }

        public MovieWritable(String movieName, Long earnings, String actor1, String actor2, String actor3, String director, String genre, float percentActor, float percentDirector) {
            this.movieName = movieName;
            this.earnings = earnings;
            this.actor1 = actor1;
            this.actor2 = actor2;
            this.actor3 = actor3;
            this.director = director;
            this.genre = genre;
            this.percentActor = percentActor;
            this.percentDirector = percentDirector;
        }

        public Long getEarnings() {
            return earnings;
        }

        public String getMovieName() {
            return movieName;
        }

        public String getGenre() {
            return genre;
        }

        public float getPercentActor() {
            return percentActor;
        }

        public float getPercentDirector() {
            return percentDirector;
        }

        public String getActor1() {
            return actor1;
        }

        public String getActor2() {
            return actor2;
        }

        public String getActor3() {
            return actor3;
        }

        public String getDirector() {
            return director;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(movieName);
            out.writeLong(earnings);
            out.writeUTF(actor1);
            out.writeUTF(actor2);
            out.writeUTF(actor3);
            out.writeUTF(director);
            out.writeUTF(genre);
            out.writeFloat(percentActor);
            out.writeFloat(percentDirector);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            movieName = in.readUTF();
            earnings = in.readLong();
            actor1 = in.readUTF();
            actor2 = in.readUTF();
            actor3 = in.readUTF();
            director = in.readUTF();
            genre = in.readUTF();
            percentActor = in.readFloat();
            percentDirector = in.readFloat();
        }

        @Override
        public String toString() {
            return movieName + "\t" + genre + "\t" + earnings + "\t" + actor1 + "\t" + actor2 + "\t" + actor3 + "\t" + director + "\t" + percentActor + "\t" + percentDirector;
        }
    }

// --------------------------- JOB 2 ---------------------- //
// -------------------------------------------------------- //   NAO TA FEITO NAO SEI OQ TO FAZNNDO

    public static class MapHighestBoxOffice extends Mapper<LongWritable, Text, Text, HighestBoxOfficeWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] fields = line.split("\t");

                if (fields.length >= 9) {
                    String genre = fields[2];
                    String director = fields[7];
                    float percentActor = Float.parseFloat(fields[8]);
                    float percentDirector = Float.parseFloat(fields[9]);
                    String actor1 = fields[4];
                    String actor2 = fields[5];
                    String actor3 = fields[6];

                    con.write(new Text(genre),
                            new HighestBoxOfficeWritable(percentActor, percentDirector, actor1, actor2, actor3, director));
                }
            }
        }
    }


    public static class ReduceHighestBoxOffice extends Reducer<Text,HighestBoxOfficeWritable,Text,HighestBoxOfficeWritable>{
        public void reduce(Text key, Iterable<HighestBoxOfficeWritable> values, Context con) throws IOException, InterruptedException {
            HighestBoxOfficeWritable HighestBoxOffice = null;
            float avg_highest_box_office = 0f;

            for (HighestBoxOfficeWritable value : values) {
                float avg_temp = (value.getPercentActor() + value.getPercentDirector()) / 2.0f;
                if (HighestBoxOffice == null || avg_temp > avg_highest_box_office) {
                    avg_highest_box_office = avg_temp;
                    HighestBoxOffice = new HighestBoxOfficeWritable(value.getPercentActor(),value.getPercentDirector(),
                            value.getActor1(), value.getActor2(), value.getActor3(),value.getDirector());
                }
            }
            if (HighestBoxOffice != null) {
                con.write(key, HighestBoxOffice);
            }
        }
    }


    public static class HighestBoxOfficeWritable implements Writable {
        private float percentActor;
        private float percentDirector;
        private String actor1;
        private String actor2;
        private String actor3;
        private String director;

        public HighestBoxOfficeWritable() {
        }

        public float getPercentActor() {
            return percentActor;
        }

        public void setPercentActor(float percentActor) {
            this.percentActor = percentActor;
        }

        public float getPercentDirector() {
            return percentDirector;
        }

        public void setPercentDirector(float percentDirector) {
            this.percentDirector = percentDirector;
        }

        public String getActor1() {
            return actor1;
        }

        public void setActor1(String actor1) {
            this.actor1 = actor1;
        }

        public String getActor2() {
            return actor2;
        }

        public void setActor2(String actor2) {
            this.actor2 = actor2;
        }

        public String getActor3() {
            return actor3;
        }

        public void setActor3(String actor3) {
            this.actor3 = actor3;
        }

        public String getDirector() {
            return director;
        }

        public void setDirector(String director) {
            this.director = director;
        }

        public HighestBoxOfficeWritable(float percentActor, float percentDirector, String actor1, String actor2, String actor3, String director) {
            this.percentActor = percentActor;
            this.percentDirector = percentDirector;
            this.actor1 = actor1;
            this.actor2 = actor2;
            this.actor3 = actor3;
            this.director = director;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeFloat(percentActor);
            dataOutput.writeFloat(percentDirector);
            dataOutput.writeUTF(actor1);
            dataOutput.writeUTF(actor2);
            dataOutput.writeUTF(actor3);
            dataOutput.writeUTF(director);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            percentActor = dataInput.readFloat();
            percentDirector = dataInput.readFloat();
            actor1 = dataInput.readUTF();
            actor2 = dataInput.readUTF();
            actor3 = dataInput.readUTF();
            director = dataInput.readUTF();
        }

        @Override
        public String toString() {
            return percentActor + "\t" + percentDirector + "\t" + actor1 + "\t" + actor2 + "\t" + actor3 + "\t" + director;
        }
    }
}