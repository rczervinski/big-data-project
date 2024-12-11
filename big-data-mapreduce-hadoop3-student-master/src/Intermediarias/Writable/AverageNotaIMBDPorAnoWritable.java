package Intermediarias.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageNotaIMBDPorAnoWritable implements Writable {
    private int count;
    private float nota_imdb;

    public AverageNotaIMBDPorAnoWritable() {
    }

    public AverageNotaIMBDPorAnoWritable(int count, float nota_imdb) {
        this.count = count;
        this.nota_imdb = nota_imdb;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public float getNota_imdb() {
        return nota_imdb;
    }

    public void setNota_imdb(float nota_imdb) {
        this.nota_imdb = nota_imdb;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.count);
        dataOutput.writeFloat(this.nota_imdb);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count = dataInput.readInt();
        this.nota_imdb = dataInput.readFloat();
    }
}
