package mapreduce.secondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 把第一列整数和第二列作为类的属性，并且实现WritableComparable接口
 */
public class IntPair implements WritableComparable<IntPair> {
  private int first = 0;
  private int second = 0;

  public void set(int left, int right) {
    first = left;
    second = right;
  }
  public int getFirst() {
    return first;
  }
  public int getSecond() {
    return second;
  }

  //反序列化，从流中的二进制转换成IntPair
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }
  //序列化，将IntPair转化成使用流传送的二进制
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }
  //默认的分区类 HashPartitioner，使用此方法
  @Override
  public int hashCode() {
    return first+"".hashCode() + second+"".hashCode();
  }
  @Override
  public boolean equals(Object right) {
    if (right instanceof IntPair) {
      IntPair r = (IntPair) right;
      return r.first == first && r.second == second;
    } else {
      return false;
    }
  }
  //这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法
  public int compareTo(IntPair o) {
    if (first != o.first) {
      return first - o.first;
    } else if (second != o.second) {
      return second - o.second;
    } else {
      return 0;
    }
  }
}

