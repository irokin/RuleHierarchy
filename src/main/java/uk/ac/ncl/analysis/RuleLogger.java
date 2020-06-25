package uk.ac.ncl.analysis;

import uk.ac.ncl.Settings;
import uk.ac.ncl.utils.MathUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Logging the number of rules produced at different steps over
 * all evaluated targets.
 */
public class RuleLogger {

    /**
     * All generated Abstract Rules (AR) from generalization.
     */
    public static Map<String, Integer> ARs = new HashMap<>();

    public static synchronized void updateARs(int size) {
        ARs.put(Settings.TARGET, size);
    }

    public static synchronized int getARs() {
        return ARs.get(Settings.TARGET);
    }

    /**
     * Stores unqualified abstract rules (UAR). There are two types of UAR:
     * - unqualified CAR
     * - Template that has no quality instantiated rules (all instantiations are deemed unqualified).
     */
    public static Map<String, Integer> unqualifiedARs = new HashMap<>();

    public static synchronized void updateUnqualifiedARs() {
        if(!unqualifiedARs.containsKey(Settings.TARGET))
            unqualifiedARs.put(Settings.TARGET, 1);
        else
            unqualifiedARs.put(Settings.TARGET, unqualifiedARs.get(Settings.TARGET) + 1);
    }

    public static synchronized int getUnqualifiedARs() {
        return unqualifiedARs.get(Settings.TARGET) == null ? 0 : unqualifiedARs.get(Settings.TARGET);
    }

    /**
     * All qualified Instantiated Rules (IR) from specialization.
     */
    public static Map<String, Integer> qualifiedIRs = new HashMap<>();

    public static synchronized void updateQualifiedIRs() {
        if (!qualifiedIRs.containsKey(Settings.TARGET))
            qualifiedIRs.put(Settings.TARGET, 1);
        else
            qualifiedIRs.put(Settings.TARGET, qualifiedIRs.get(Settings.TARGET) + 1);
    }

    public static synchronized int getQualifiedIRs() {
        return qualifiedIRs.get(Settings.TARGET) == null ? 0 : qualifiedIRs.get(Settings.TARGET);
    }

    /**
     * All qualified ARs in specialization.
     */
    public static Map<String, Integer> qualifiedARs = new HashMap<>();

    public static synchronized void updateQualifiedARs() {
        if(!qualifiedARs.containsKey(Settings.TARGET))
            qualifiedARs.put(Settings.TARGET, 1);
        else
            qualifiedARs.put(Settings.TARGET, qualifiedARs.get(Settings.TARGET) + 1);
    }

    public static synchronized int getQualifiedARs() {
        return qualifiedARs.get(Settings.TARGET) == null ? 0 : qualifiedARs.get(Settings.TARGET);
    }


    /**
     * All unqualified instantiated rules.
     */
    public static Map<String, Integer> unqualifiedIRs = new HashMap<>();
    public static synchronized void updateUnqualifiedIRs() {
        if(!unqualifiedIRs.containsKey(Settings.TARGET))
            unqualifiedIRs.put(Settings.TARGET, 1);
        else
            unqualifiedIRs.put(Settings.TARGET, unqualifiedIRs.get(Settings.TARGET) + 1);
    }

    public static synchronized int getUnqualifiedIRs() {
        return unqualifiedIRs.get(Settings.TARGET) == null ? 0 : unqualifiedIRs.get(Settings.TARGET);
    }

    /**
     * Prior pruned abstract rules.
     */
    public static Map<String, Integer> priorPrunedARs = new HashMap<>();
    public static synchronized void updatePriorPrunedARs(int pruned) {
        if(!priorPrunedARs.containsKey(Settings.TARGET))
            priorPrunedARs.put(Settings.TARGET, pruned);
        else
            priorPrunedARs.put(Settings.TARGET, priorPrunedARs.get(Settings.TARGET) + pruned);
    }
    public static synchronized int getPriorPrunedARs() {
        return priorPrunedARs.get(Settings.TARGET) == null ? 0 : priorPrunedARs.get(Settings.TARGET);
    }

    /**
     * Post pruned instantiated rules.
     */
    public static Map<String, Integer> postPrunedIRs = new HashMap<>();
    public static synchronized void updatePostPrunedIRs() {
        if(!postPrunedIRs.containsKey(Settings.TARGET))
            postPrunedIRs.put(Settings.TARGET, 1);
        else
            postPrunedIRs.put(Settings.TARGET, postPrunedIRs.get(Settings.TARGET) + 1);
    }
    public static synchronized int getPostPrunedIRs() {
        return postPrunedIRs.get(Settings.TARGET) == null ? 0 : postPrunedIRs.get(Settings.TARGET);
    }

    public static double reportAvg(Map<String, Integer> map) {
        List<Double> list = new ArrayList<>();
        for (Integer value : map.values()) {
            list.add((double) value);
        }
        return MathUtils.listMean(list);
    }

    public static Map<String, Integer> IRs = new HashMap<>();

    public static int getIRs() {
        IRs.put(Settings.TARGET, getQualifiedIRs() + getUnqualifiedIRs() + getPostPrunedIRs());
        return IRs.get(Settings.TARGET);
    }

    static Map<String, Boolean> constraintsTriggered = new HashMap<>();

    public static boolean isConstraintsTriggered() {
        constraintsTriggered.put(Settings.TARGET, getARs() != getQualifiedARs() + getUnqualifiedARs() + getPriorPrunedARs());
        return constraintsTriggered.get(Settings.TARGET);
    }

    public static String constraintsTriggeredCount() {
        int count = 0;
        for (Boolean value : constraintsTriggered.values()) {
            if(value)
                count++;
        }
        return count + "/" + constraintsTriggered.values().size();
    }

    /**
     * All visited ARs during specialization.
     */
    public static Map<String, Integer> specializedARs = new HashMap<>();

    public static synchronized int getSpecializedARs() {
        specializedARs.put(Settings.TARGET, getQualifiedARs() + getUnqualifiedARs() + getPriorPrunedARs());
        return specializedARs.get(Settings.TARGET);
    }

}
