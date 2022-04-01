import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Mobile implements Writable {

    private String phone;
    private long upStream;
    private long downStream;
    private long sumStream;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getDownStream() {
        return downStream;
    }

    public long getUpStream() {
        return upStream;
    }

    public long getSumStream() {
        return sumStream;
    }

    public void setDownStream(long downStream) {
        this.downStream = downStream;
    }

    public void setSumStream(long sumStream) {
        this.sumStream = sumStream;
    }

    public void setUpStream(long upStream) {
        this.upStream = upStream;
    }

    public Mobile() {}

    public Mobile(String phone, long upStream, long downStream) {
        super();
        this.phone = phone;
        this.upStream = upStream;
        this.downStream = downStream;
        this.sumStream = upStream + downStream;
    }

    public void readFields(DataInput input) throws IOException {
        phone = input.readUTF();
        upStream = input.readLong();
        downStream = input.readLong();
        sumStream = input.readLong();
    }

    public void write(DataOutput output) throws IOException {
        output.writeUTF(phone);
        output.writeLong(upStream);
        output.writeLong(downStream);
        output.writeLong(sumStream);
    }

    public String toString() {
        return "" + upStream + "\t" + downStream + "\t" + sumStream;
    }
}

