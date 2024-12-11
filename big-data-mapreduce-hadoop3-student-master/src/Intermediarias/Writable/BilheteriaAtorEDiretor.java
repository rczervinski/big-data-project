package Intermediarias.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BilheteriaAtorEDiretor implements Writable {
    private String ator;
    private float bilheteria;

    public BilheteriaAtorEDiretor() {
    }

    public BilheteriaAtorEDiretor(String ator, float bilheteria) {
        this.ator = ator;
        this.bilheteria = bilheteria;
    }

    public String getAtor() {
        return ator;
    }

    public void setAtor(String ator) {
        this.ator = ator;
    }

    public double getBilheteria() {
        return bilheteria;
    }

    public void setBilheteria(int bilheteria) {
        this.bilheteria = bilheteria;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ator);
        dataOutput.writeFloat(bilheteria);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ator = dataInput.readUTF();
        this.bilheteria = dataInput.readFloat();
    }
}