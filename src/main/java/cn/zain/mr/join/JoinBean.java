package cn.zain.mr.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yongz on 2018/2/24.
 */
public class JoinBean implements Writable {
    private String orderId="null";
    private String uid="null";
    private String username="null";
    private String age="null";
    private String lover="null";
    private String tableName="null";

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getLover() {
        return lover;
    }

    public void setLover(String lover) {
        this.lover = lover;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.uid);
        dataOutput.writeUTF(this.username);
        dataOutput.writeUTF(this.age);
        dataOutput.writeUTF(this.lover);
        dataOutput.writeUTF(this.tableName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.uid = dataInput.readUTF();
        this.username = dataInput.readUTF();
        this.age = dataInput.readUTF();
        this.lover = dataInput.readUTF();
        this.tableName = dataInput.readUTF();
    }
}
