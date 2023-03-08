package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CustomClassWritable implements Comparable<CustomClassWritable>, Writable {
    private String pid; // Contains a "word"
	private Float score; // number of occurrences of "word"

	public CustomClassWritable(String pid, Float score) {
		this.pid = pid;
		this.score = score;
	}

	public CustomClassWritable(CustomClassWritable other) {
		this.pid = new String(other.getPid());
		this.score = Float.valueOf(other.getScore());
	}

	public CustomClassWritable() {
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public Float getScore() {
		return score;
	}

	public void setScore(Float score) {
		this.score = score;
	}

	@Override
	public int compareTo(CustomClassWritable other) {

		if (this.score.compareTo(other.getScore()) != 0) {
			return this.score.compareTo(other.getScore());
		} else { // if the count values of the two words are equal, the
					// lexicographical order is considered
			return this.pid.compareTo(other.getPid());
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pid = in.readUTF();
		score = in.readFloat();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(pid);
		out.writeFloat(score);

	}

	@Override
	public String toString() {
		return new String(pid + "," + score);
	}
}
