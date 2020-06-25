package uk.ac.ncl.model;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import uk.ac.ncl.Hierarchy.RuleHierarchy;
import uk.ac.ncl.RHSettings;
import uk.ac.ncl.Settings;
import uk.ac.ncl.core.*;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.utils.Logger;

import java.io.File;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class RH extends Engine {

    public RH(File config, String logName) {
        super(config, logName);
        RHSettings.USE_PRIOR_PRUNE = Helpers.readSetting(args, "use_prior_prune", RHSettings.USE_PRIOR_PRUNE);
        RHSettings.PRIOR_PRUNE_TH = Helpers.readSetting(args, "prior_th", RHSettings.PRIOR_PRUNE_TH);
        RHSettings.USE_POST_PRUNE = Helpers.readSetting(args, "use_post_prune", RHSettings.USE_POST_PRUNE);
        RHSettings.FILTER_UNSOLVABLE = Helpers.readSetting(args, "filter_unsolvable", RHSettings.FILTER_UNSOLVABLE);
        Helpers.reportRHSettings();
    }

    public void run() {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        testFile = new File(home, "data/annotated_test.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        ruleIndexHome = new File(out, "index");
        ruleIndexHome.mkdir();

        populateTargets();
        if(RHSettings.FILTER_UNSOLVABLE)
            IO.populateUnsolvable(trainFile, validFile, testFile);
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = IO.createEmptyFile(new File(ruleIndexHome
                    , target.replaceAll("[:/]", "_") + ".txt"));
            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Settings.TARGET_FUNCTIONAL = IO.isTargetFunctional(trainPairs);

                Set<Pair> validPairs = IO.readPair(graph, validFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                FilterSet.initFilterSet(trainPairs, validPairs, testPairs);

                Logger.println(MessageFormat.format("# Functional: {0} | Train Size: {1} | Valid Size: {2} | Test Size: {3}"
                        , Settings.TARGET_FUNCTIONAL, trainPairs.size(), validPairs.size(), testPairs.size()), 1);

                generalization(trainPairs, context);
                RuleHierarchy inHierarchy = new RuleHierarchy(context.getAbstractRules());
                specialization(context, trainPairs, validPairs, ruleIndexFile, inHierarchy);
                Helpers.reportLocalRuleAnalysis();
                IO.orderRuleIndexFile(ruleIndexFile);
                ruleApplication(context, ruleIndexFile);

                if(Settings.RULE_GRAPH) {
                    ModelSelection.selectModel(args, context);
                    ModelSelection.buildRuleGraph(ruleGraph, graph, context);
                }

                Evaluator evaluator = new Evaluator(testPairs, FilterSet.buildFilterSet()
                        , context, predictionFile, verificationFile, graph);
                evaluator.createQueries();
                tx.success();
            }
        }

        Logger.println("\n# Global Analysis:");
        Helpers.reportGlobalRuleAnalysis();
        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();

        Logger.init(new File(out, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home.getPath());
        Evaluator.scoreGPFL(filterMap, predictionFile);
    }

    public void learn() {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        ruleFile = IO.createEmptyFile(new File(out, "rules.txt"));
        ruleIndexHome = new File(out, "index");
        ruleIndexHome.mkdir();

        populateTargets();
        if (RHSettings.FILTER_UNSOLVABLE)
            IO.populateUnsolvable(trainFile, validFile);
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = IO.createEmptyFile(new File(ruleIndexHome
                    , target.replaceAll("[:/]", "_") + ".txt"));
            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Settings.TARGET_FUNCTIONAL = IO.isTargetFunctional(trainPairs);

                Set<Pair> validPairs = IO.readPair(graph, validFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                FilterSet.initFilterSet(trainPairs, validPairs, testPairs);

                Logger.println(MessageFormat.format("# Functional: {0} | Train Size: {1} | Valid Size: {2} | Test Size: {3}"
                        , Settings.TARGET_FUNCTIONAL, trainPairs.size(), validPairs.size(), testPairs.size()), 1);

                generalization(trainPairs, context);
                RuleHierarchy inHierarchy = new RuleHierarchy(context.getAbstractRules());
                specialization(context, trainPairs, validPairs, ruleIndexFile, inHierarchy);
                Helpers.reportLocalRuleAnalysis();
                IO.orderRuleIndexFile(ruleIndexFile);
                tx.success();
            }
        }

        Logger.println("\n# Global Analysis:");
        Helpers.reportGlobalRuleAnalysis();
        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();
    }

    public void apply() {
        graphFile = new File(home, "databases/graph.db");
        graph = IO.loadGraph(graphFile);
        trainFile = new File(home, "data/annotated_train.txt");
        validFile = new File(home, "data/annotated_valid.txt");
        testFile = new File(home, "data/annotated_test.txt");

        predictionFile = IO.createEmptyFile(new File(out, "predictions.txt"));
        verificationFile = IO.createEmptyFile(new File(out, "verifications.txt"));
        ruleIndexHome = new File(out, "index");
        populateTargets();
        if(RHSettings.FILTER_UNSOLVABLE)
            IO.populateUnsolvable(trainFile, validFile, testFile);
        GlobalTimer.programStartTime = System.currentTimeMillis();

        for (String target : targets) {
            File ruleIndexFile = new File(ruleIndexHome
                    , target.replaceAll("[:/]", "_") + ".txt");
            if(!ruleIndexFile.exists())
                continue;

            Settings.TARGET = target;
            Context context = new Context();
            Logger.println(MessageFormat.format("\n# ({0}\\{1}) Start Learning Rules for Target: {2}",
                    globalTargetCounter++, targets.size(), target), 1);

            try (Transaction tx = graph.beginTx()) {
                Set<Pair> trainPairs = IO.readPair(graph, trainFile, target);
                Set<Pair> validPairs = IO.readPair(graph, validFile, target);
                Set<Pair> testPairs = IO.readPair(graph, testFile, target);
                FilterSet.initFilterSet(trainPairs, validPairs, testPairs);

                Logger.println(MessageFormat.format("# Functional: {0} | Train Size: {1} | Valid Size: {2} | Test Size: {3}"
                        , Settings.TARGET_FUNCTIONAL, trainPairs.size(), validPairs.size(), testPairs.size()), 1);

                ruleApplication(context, ruleIndexFile);
                if(Settings.RULE_GRAPH) {
                    ModelSelection.selectModel(args, context);
                    ModelSelection.buildRuleGraph(ruleGraph, graph, context);
                }

                Evaluator evaluator = new Evaluator(testPairs, FilterSet.buildFilterSet()
                        , context, predictionFile, verificationFile, graph);
                evaluator.createQueries();
                tx.success();
            }
        }

        Logger.println("\n# Global Analysis:");
        GlobalTimer.reportMaxMemoryUsed();
        GlobalTimer.reportTime();

        Logger.init(new File(out, "eval_log.txt"), false);
        Multimap<String, Triple> filterMap = Evaluator.buildFilterMap(home.getPath());
        Evaluator.scoreGPFL(filterMap, predictionFile);
    }

    public void specialization(Context context, Set<Pair> trainPairs, Set<Pair> validPairs
            , File ruleIndexFile
            , RuleHierarchy hierarchy) {
        NumberFormat f = NumberFormat.getNumberInstance(Locale.US);
        long s = System.currentTimeMillis();

        Multimap<Long, Long> objOriginalMap = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> subOriginalMap = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair trainPair : trainPairs) {
            objOriginalMap.put(trainPair.objId, trainPair.subId);
            subOriginalMap.put(trainPair.subId, trainPair.objId);
        }

        Multimap<Long, Long> validObjToSub = MultimapBuilder.hashKeys().hashSetValues().build();
        Multimap<Long, Long> validSubToObj = MultimapBuilder.hashKeys().hashSetValues().build();
        for (Pair validPair : validPairs) {
            validObjToSub.put(validPair.objId, validPair.subId);
            validSubToObj.put(validPair.subId, validPair.objId);
        }

        BlockingQueue<Rule> abstractRuleQueue = RHSettings.USE_PRIOR_PRUNE ?
                new LinkedBlockingDeque<>(hierarchy.initQueryMap()) :
                new LinkedBlockingDeque<>(context.sortTemplates());

        BlockingQueue<String> tempFileContents = new LinkedBlockingDeque<>(1000000);
        BlockingQueue<String> ruleFileContents = new LinkedBlockingDeque<>(1000000);

        GlobalTimer.setSpecStartTime(System.currentTimeMillis());
        SpecializationTask[] tasks = new SpecializationTask[Settings.THREAD_NUMBER];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new SpecializationTask(i, graph, abstractRuleQueue
                    , trainPairs, validPairs, objOriginalMap, subOriginalMap, validObjToSub, validSubToObj
                    , context, tempFileContents, ruleFileContents, hierarchy);
        }
        RuleWriter tempFileWriter = new RuleWriter(0, tasks, ruleIndexFile, tempFileContents, true);
        RuleWriter ruleFileWriter = new RuleWriter(0, tasks, ruleFile, ruleFileContents, true);
        try {
            for (SpecializationTask task : tasks) {
                task.join();
            }
            tempFileWriter.join();
            ruleFileWriter.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        GlobalTimer.updateInsRuleStats(Helpers.timerAndMemory(s,"# Specialization"));
        Logger.println(Context.analyzeRuleComposition("# Qualified Abstract Rules", context.getSpecializedRules()), 1);
    }

    static class SpecializationTask extends Thread {
        int id;
        GraphDatabaseService graph;
        BlockingQueue<Rule> abstractRuleQueue;
        BlockingQueue<String> tempFileContents;
        Context context;
        Set<Pair> trainPairs;
        Set<Pair> validPairs;
        Multimap<Long, Long> objOriginalMap;
        Multimap<Long, Long> subOriginalMap;
        Multimap<Long, Long> validObjToSub;
        Multimap<Long, Long> validSubToObj;
        BlockingQueue<String> ruleFileContents;
        RuleHierarchy hierarchy;

        public SpecializationTask(int id
                , GraphDatabaseService graph
                , BlockingQueue<Rule> abstractRuleQueue
                , Set<Pair> trainPairs
                , Set<Pair> validPairs
                , Multimap<Long, Long> objOriginalMap
                , Multimap<Long, Long> subOriginalMap
                , Multimap<Long, Long> validObjToSub
                , Multimap<Long, Long> validSubToObj
                , Context context
                , BlockingQueue<String> tempFileContents
                , BlockingQueue<String> ruleFileContents
                , RuleHierarchy hierarchy) {
            super("InstantiationTask-" + id);
            this.id = id;
            this.graph = graph;
            this.abstractRuleQueue = abstractRuleQueue;
            this.tempFileContents = tempFileContents;
            this.trainPairs = trainPairs;
            this.objOriginalMap = objOriginalMap;
            this.subOriginalMap = subOriginalMap;
            this.context = context;
            this.ruleFileContents = ruleFileContents;
            this.validObjToSub = validObjToSub;
            this.validSubToObj = validSubToObj;
            this.validPairs = validPairs;
            this.hierarchy = hierarchy;
            start();
        }

        @Override
        public void run() {
            try(Transaction tx = graph.beginTx()) {
                while (!abstractRuleQueue.isEmpty() && !GlobalTimer.stopSpec() && context.getTotalInsRules() < Settings.INS_RULE_CAP) {
                    Template abstractRule = (Template) abstractRuleQueue.poll();
                    if(abstractRule != null) {
                        Multimap<Long, Long> anchoringToOriginalMap = abstractRule.isFromSubject() ? objOriginalMap : subOriginalMap;
                        Multimap<Long, Long> validOriginals = abstractRule.isFromSubject() ? validObjToSub : validSubToObj;
                        abstractRule.specializationWithHierarchy(graph
                                , trainPairs
                                , validPairs
                                , anchoringToOriginalMap
                                , validOriginals
                                , context
                                , ruleFileContents
                                , tempFileContents
                                , hierarchy
                                , abstractRuleQueue);
                    }
                }
                tx.success();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

}
