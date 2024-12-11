package Avancadas.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfluenciaDiretorWritable implements Writable {
    private int quantidade_filmes;
    private Double media_ganhos;
    private Double media_oscar;

    public InfluenciaDiretorWritable() {
    }



    public InfluenciaDiretorWritable(Double media_ganhos, Double media_oscar, int quantidade_filmes) {
        this.media_ganhos = media_ganhos;
        this.media_oscar = media_oscar;
        this.quantidade_filmes = quantidade_filmes;
    }

    public int getQuantidade_filmes() {
        return quantidade_filmes;
    }

    public void setQuantidade_filmes(int quantidade_filmes) {
        this.quantidade_filmes = quantidade_filmes;
    }

    public Double getMedia_ganhos() {
        return media_ganhos;
    }

    public void setMedia_ganhos(Double media_ganhos) {
        this.media_ganhos = media_ganhos;
    }

    public Double getMedia_oscar() {
        return media_oscar;
    }

    public void setMedia_oscar(Double media_oscar) {
        this.media_oscar = media_oscar;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(media_ganhos);
        dataOutput.writeDouble(media_oscar);
        dataOutput.writeInt(quantidade_filmes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.media_ganhos = dataInput.readDouble();
        this.media_oscar = dataInput.readDouble();
        this.quantidade_filmes = dataInput.readInt();
    }

    @Override
    public String toString() {
        return String.format("ganhou uma média de %.2f Oscar(s) e faturou %.2f por filme", media_oscar, media_ganhos);
    }
}
