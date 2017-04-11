package MapReduceJobs;

import java.util.Comparator;

public class CustomComparator implements Comparator<Tuple<String,Double>> {
    @Override
    public int compare(Tuple<String,Double> o1, Tuple<String,Double> o2) {
        return o2.y.compareTo(o1.y);
    }
}