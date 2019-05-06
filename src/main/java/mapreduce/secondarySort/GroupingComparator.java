package mapreduce.secondarySort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 在分组比较的时候，只比较原来的key，而不是组合key。
 */
public class GroupingComparator implements RawComparator<IntPair> {
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
  }

  public int compare(IntPair o1, IntPair o2) {
    int first1 = o1.getFirst();
    int first2 = o2.getFirst();
    return first1 - first2;
  }
}
