package Intermediarias.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgForIMDBWritable implements Writable {
    private float imdb;
    private int sum;

    public AvgForIMDBWritable() {}

    public AvgForIMDBWritable(float imdb, int sum){
        this.imdb = imdb;
        this.sum = sum;
    }

    public float getImdb() {
        return imdb;
    }

    public void setImdb(float imdb) {
        this.imdb = imdb;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(imdb);
        dataOutput.writeInt(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.imdb = dataInput.readFloat();
        this.sum = dataInput.readInt();
    }
}