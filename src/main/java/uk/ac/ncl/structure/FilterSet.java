package uk.ac.ncl.structure;

import java.util.HashSet;
import java.util.Set;

public class FilterSet {
    private static Set<Pair> trainPairs;
    private static Set<Pair> validPairs;
    private static Set<Pair> testPairs;

    public static void initFilterSet(Set<Pair> trainPairs, Set<Pair> validPairs, Set<Pair> testPairs) {
        FilterSet.trainPairs = trainPairs;
        FilterSet.validPairs = validPairs;
        FilterSet.testPairs = testPairs;
    }

    public static Set<Pair> buildFilterSet() {
        Set<Pair> pairs = new HashSet<>();
        pairs.addAll(trainPairs);
        pairs.addAll(validPairs);
        pairs.addAll(testPairs);
        return pairs;
    }

    public static boolean isKnown(Pair pair) {
        if(trainPairs.contains(pair))
            return true;
        return validPairs.contains(pair);
    }

    public static boolean isKnownWithTest(Pair pair) {
        if(trainPairs.contains(pair))
            return true;
        if(validPairs.contains(pair))
            return true;
        return testPairs.contains(pair);
    }

    public static boolean inTestSet(Pair pair) {
        return testPairs.contains(pair);
    }

    public static int testSetSize() {
        return testPairs.size();
    }

    public static int size() {
        return trainPairs.size() + validPairs.size() + testPairs.size();
    }
}
