package Avancadas.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DiretorComMediaDeBilheteria implements Writable {

    private Double porcentagemBilheteria;
    private int sum;
    private Double oscar;
    private Double lucro;

    public Double getOscar() {
        return oscar;
    }

    public void setOscar(Double oscar) {
        this.oscar = oscar;
    }

    public Double getLucro() {
        return lucro;
    }

    public void setLucro(Double lucro) {
        this.lucro = lucro;
    }

    public DiretorComMediaDeBilheteria() {
    }

    public DiretorComMediaDeBilheteria(Double porcentagemBilheteria, int sum, Double lucro, Double oscar) {
        this.porcentagemBilheteria = porcentagemBilheteria;
        this.sum = sum;
        this.lucro = lucro;
        this.oscar = oscar;
    }

    public Double getPorcentagemBilheteria() {
        return porcentagemBilheteria;
    }

    public void setPorcentagemBilheteria(Double porcentagemBilheteria) {
        this.porcentagemBilheteria = porcentagemBilheteria;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sum);
        dataOutput.writeDouble(porcentagemBilheteria);
        dataOutput.writeDouble(lucro);
        dataOutput.writeDouble(oscar);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum = dataInput.readInt();
        this.porcentagemBilheteria = dataInput.readDouble();
        this.lucro = dataInput.readDouble();
        this.oscar = dataInput.readDouble();
    }

    @Override
    public String toString(){
        return String.format("%.2f %d %.2f %.2f",
                porcentagemBilheteria, sum, lucro, oscar);
    }
}
