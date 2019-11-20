package com.jiangtao.step7;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Result implements WritableComparable<Result>, DBWritable {

    private String uid;
    private String gid;
    private int exp;

    @Override
    public int compareTo(Result o) {
        int n = Integer.parseInt(this.uid) - Integer.parseInt(o.getUid());
        if(n != 0) return n;
        return Integer.parseInt(this.gid) - Integer.parseInt(o.getGid());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uid);
        out.writeUTF(this.gid);
        out.writeInt(this.exp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.gid = in.readUTF();
        this.exp = in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,this.uid);
        statement.setString(2,this.gid);
        statement.setInt(3,this.exp);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        if(resultSet == null) return;
        this.uid = resultSet.getString(1);
        this.gid = resultSet.getString(2);
        this.exp = resultSet.getInt(3);
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
    }

    public int getExp() {
        return exp;
    }

    public void setExp(int exp) {
        this.exp = exp;
    }

    public Result(String uid, String gid, int exp) {
        this.uid = uid;
        this.gid = gid;
        this.exp = exp;
    }

    public Result() {
    }
}
