package Intermediarias.Writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import Intermediarias.Writable.*;

public class MovieCombination implements WritableComparable<MovieCombination> {
    private Text actor1 = new Text();
    private Text director = new Text();

    public MovieCombination() {}

    public MovieCombination(String actor1, String director) {
        this.actor1.set(actor1);
        this.director.set(director);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        actor1.write(out);
        director.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        actor1.readFields(in);
        director.readFields(in);
    }

    @Override
    public int compareTo(MovieCombination o) {
        int cmp = actor1.compareTo(o.actor1);
        if (cmp != 0) return cmp;
        return director.compareTo(o.director);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieCombination that = (MovieCombination) o;
        return actor1.equals(that.actor1) && director.equals(that.director);
    }

    @Override
    public int hashCode() {
        return actor1.hashCode() * 31 + director.hashCode();
    }

    @Override
    public String toString() {
        return actor1 + "," + director;
    }
}