package Storage;

import java.util.Arrays;

/**
 * @author Achilles
 */
public class SortedSetEntry implements Comparable<SortedSetEntry>{
    public final double score;
    public final byte[] member;

    public SortedSetEntry(double score, byte[] member){
        this.score = score;
        this.member = member;
    }

    @Override
    public int compareTo(SortedSetEntry other) {
        //首先按分数排序
        int scoreCompare = Double.compare(this.score, other.score);
        if(scoreCompare!=0){
            return scoreCompare;
        }
        //如果分数相同，按成员的字典序排序
        return new String(this.member).compareTo(new String(other.member));
    }
    @Override
    public boolean equals(Object obj) {
        if(this==obj) {
            return true;
        }

        if(obj==null||getClass()!=obj.getClass()){
            return false;
        }

        SortedSetEntry that=(SortedSetEntry)obj;

        return Arrays.equals(this.member, that.member);
    }
    @Override
    public int hashCode() {
        return Arrays.hashCode(member);
    }
}
