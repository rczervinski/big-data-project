package Intermediarias.Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EarningStats implements Writable {
    private long earnings;
    private int movieCount;

    public EarningStats() {}

    public EarningStats(long earnings, int movieCount) {
        this.earnings = earnings;
        this.movieCount = movieCount;
    }

    public long getEarnings() {
        return earnings;
    }

    public int getMovieCount() {
        return movieCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(earnings);
        out.writeInt(movieCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        earnings = in.readLong();
        movieCount = in.readInt();
    }
}
