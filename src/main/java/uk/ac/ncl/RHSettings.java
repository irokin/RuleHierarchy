package uk.ac.ncl;

public class RHSettings {

    /**
     * Turn on abstract hierarchy for pruning
     */
    public static boolean USE_PRIOR_PRUNE = false;

    /**
     * Monotonic measure (support) threshold
     */
    public static int PRIOR_PRUNE_TH = 10;

    /**
     * Turn on deductive hierarchy for pruning
     */
    public static boolean USE_POST_PRUNE = false;

    /**
     * Filter unsolvable test and valid triples (triples that contain
     * entities that have no connections in the training set, thus no
     * rules or regularities can suggest them.)
     */
    public static boolean FILTER_UNSOLVABLE = false;
}
