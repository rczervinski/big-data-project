package Avancadas.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HighestBoxOfficeWritable implements Writable {
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
