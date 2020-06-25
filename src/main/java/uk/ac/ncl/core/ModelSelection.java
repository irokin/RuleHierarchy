package uk.ac.ncl.core;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.*;
import uk.ac.ncl.Settings;
import uk.ac.ncl.structure.*;
import uk.ac.ncl.utils.IO;

import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

public class ModelSelection {

    public static Model bestModel;

    public static void selectModel(JSONObject args, Context context) {
        Multimap<Rule, Pair> map = context.rulePredictionMap;
        List<Model> models = new ArrayList<>();

        List<Integer> sizes = new ArrayList<>();
        try {
            JSONArray array = args.getJSONArray("ms_sizes");
            if(!array.isEmpty()) {
                for (Object o : array) {
                    sizes.add((Integer) o);
                }
            }
        } catch (JSONException e) {
            for (int i : new int[]{500, 1000, 3000, 5000, 10000, 20000, 30000, 50000}) {
                sizes.add(i);
            }
        }
        System.out.println("\n# Start Model Selection with Sizes: " + sizes);

        for (int size : sizes) {
            Model withConf = new Model("Confidence", map, size);
            models.add(withConf);
            withConf.report();
            Model withVPrecision = new Model("validPrecision", map, size);
            models.add(withVPrecision);
            withVPrecision.report();
            Model withLocalF1 = new Model("localF1", map, size);
            models.add(withLocalF1);
            withLocalF1.report();
        }

        models.sort((m1, m2) -> {
            double v1 = m1.getF1();
            double v2 = m2.getF1();
            return Double.compare(v2, v1);
        });

        bestModel = models.get(0);
        System.out.println("# Best Model:");
        bestModel.report();
    }

    public static void buildRuleGraph(GraphDatabaseService ruleGraph, GraphDatabaseService graph, Context context) {
        DecimalFormat f = new DecimalFormat("###.###");
        List<Rule> qualityRules = ModelSelection.bestModel.getRules();
        Set<Pair> knownTriples = new HashSet<>();
        Set<Pair> inferredTriples = new HashSet<>();
        Map<Long, Long> entityIndexMap = new HashMap<>();
        Map<Pair, List<Rule>> predictionMap = context.getPredictionMap();

        for (Pair pair : predictionMap.keySet()) {
            if(FilterSet.isKnownWithTest(pair)) knownTriples.add(pair);
        }
        for (Rule qualityRule : qualityRules) {
            for (Pair pair : context.rulePredictionMap.get(qualityRule)) {
                if(!knownTriples.contains(pair))
                    inferredTriples.add(pair);
            }
        }

        System.out.println(MessageFormat.format("\n# Build Rule Graph with {6} Quality Rules" +
                        "\n# Known: all={0} | explained={1} | ratio={2} " +
                        "\n# Inferred: all={3} | quality={4} | ratio={5}"
                , FilterSet.size()
                , knownTriples.size()
                , f.format((double) knownTriples.size() / FilterSet.size())
                , predictionMap.keySet().size()
                , inferredTriples.size()
                , f.format((double) inferredTriples.size() / predictionMap.keySet().size())
                , qualityRules.size()));

        int count = 1;
        Iterator<Pair> iterator = knownTriples.iterator();
        while (iterator.hasNext()) {
            try(Transaction tx = ruleGraph.beginTx()) {
                while (iterator.hasNext() && count++ % 10000 != 0) {
                    Pair current = iterator.next();
                    Node start, end;
                    List<Rule> rules = predictionMap.get(current);

                    if(entityIndexMap.containsKey(current.subId))
                        start = ruleGraph.getNodeById(entityIndexMap.get(current.subId));
                    else {
                        start = ruleGraph.createNode();
                        Node knownStartNode = graph.getNodeById(current.subId);
                        knownStartNode.getLabels().forEach(start::addLabel);
                        knownStartNode.getAllProperties().forEach(start::setProperty);
                        entityIndexMap.put(current.subId, start.getId());
                    }

                    if(entityIndexMap.containsKey(current.objId))
                        end = ruleGraph.getNodeById(entityIndexMap.get(current.objId));
                    else {
                        end = ruleGraph.createNode();
                        Node knownEndNode = graph.getNodeById(current.objId);
                        knownEndNode.getLabels().forEach(end::addLabel);
                        knownEndNode.getAllProperties().forEach(end::setProperty);
                        entityIndexMap.put(current.objId, end.getId());
                    }

                    Relationship rel = start.createRelationshipTo(end, RelationshipType.withName("known_" + Settings.TARGET));
                    rules.sort(IO.ruleComparatorBySC());
                    rel.setProperty("confidence", rules.get(0).getQuality());
                    rules.sort(IO.ruleComparatorByValidPrec());
                    rel.setProperty("vPrecision", rules.get(0).getValidPrecision());
                    rules.sort(IO.ruleComparatorByLocalF1());
                    rel.setProperty("localF1", rules.get(0).getLocalF1());
                    rel.setProperty("ruleCount", rules.size());

                    for (Rule rule : rules.subList(0, Math.min(rules.size(), Settings.VERIFY_RULE_SIZE))) {
                        String typeName = rule.isClosed() ? "Closed_Abstract_Rule" : rule.getType() == 0
                                ? "Head_Anchored_Rule" : "Both_Anchored_Rule";
                        Relationship ruleRel = start.createRelationshipTo(end, RelationshipType.withName(typeName));
                        if(rule instanceof SimpleInsRule)
                            ((SimpleInsRule) rule).insRuleString(graph);
                        ruleRel.setProperty("headAtom", rule.head.toString());
                        ruleRel.setProperty("bodyAtoms", rule.bodyAtoms.stream().map(Atom::toString).collect(Collectors.joining(",")));
                        ruleRel.setProperty("confidence", rule.getQuality());
                        ruleRel.setProperty("vPrecision", rule.getValidPrecision());
                        ruleRel.setProperty("localF1", rule.getLocalF1());
                    }
                }
//                System.out.println("# Finished Batch: " + (count - 1));
                tx.success();
            }
        }

        count = 1;
        iterator = inferredTriples.iterator();
        while (iterator.hasNext()) {
            try(Transaction tx = ruleGraph.beginTx()) {
                while (iterator.hasNext() && count++ % 10000 != 0) {
                    Pair current = iterator.next();
                    Node start, end;
                    List<Rule> rules = predictionMap.get(current);

                    if(entityIndexMap.containsKey(current.subId))
                        start = ruleGraph.getNodeById(entityIndexMap.get(current.subId));
                    else {
                        start = ruleGraph.createNode();
                        Node knownStartNode = graph.getNodeById(current.subId);
                        knownStartNode.getLabels().forEach(start::addLabel);
                        knownStartNode.getAllProperties().forEach(start::setProperty);
                        entityIndexMap.put(current.subId, start.getId());
                    }

                    if(entityIndexMap.containsKey(current.objId))
                        end = ruleGraph.getNodeById(entityIndexMap.get(current.objId));
                    else {
                        end = ruleGraph.createNode();
                        Node knownEndNode = graph.getNodeById(current.objId);
                        knownEndNode.getLabels().forEach(end::addLabel);
                        knownEndNode.getAllProperties().forEach(end::setProperty);
                        entityIndexMap.put(current.objId, end.getId());
                    }

                    Relationship rel = start.createRelationshipTo(end, RelationshipType.withName("inferred_" + Settings.TARGET));
                    rules.sort(IO.ruleComparatorBySC());
                    rel.setProperty("confidence", rules.get(0).getQuality());
                    rules.sort(IO.ruleComparatorByValidPrec());
                    rel.setProperty("vPrecision", rules.get(0).getValidPrecision());
                    rules.sort(IO.ruleComparatorByLocalF1());
                    rel.setProperty("localF1", rules.get(0).getLocalF1());
                    rel.setProperty("ruleCount", rules.size());

                    for (Rule rule : rules.subList(0, Math.min(rules.size(), Settings.VERIFY_RULE_SIZE))) {
                        String typeName = rule.isClosed() ? "Closed_Abstract_Rule" : rule.getType() == 0
                                ? "Head_Anchored_Rule" : "Both_Anchored_Rule";
                        Relationship ruleRel = start.createRelationshipTo(end, RelationshipType.withName(typeName));
                        if(rule instanceof SimpleInsRule)
                            ((SimpleInsRule) rule).insRuleString(graph);
                        ruleRel.setProperty("headAtom", rule.head.toString());
                        ruleRel.setProperty("bodyAtoms", rule.bodyAtoms.stream().map(Atom::toString).collect(Collectors.joining(",")));
                        ruleRel.setProperty("confidence", rule.getQuality());
                        ruleRel.setProperty("vPrecision", rule.getValidPrecision());
                        ruleRel.setProperty("localF1", rule.getLocalF1());
                    }
                }
//                System.out.println("# Finished Batch: " + (count - 1));
                tx.success();
            }
        }
    }

    public static class Model {
        double recall;
        double precision;
        Multimap<Pair, Rule> candidates;
        List<Rule> rules;
        int maxRuleSize;
        int inferred;
        int known;
        String header;

        public Model(String header, Multimap<Rule, Pair> map, int maxRuleSize) {
            this.maxRuleSize = maxRuleSize;
            int testSupport = 0;
            int filteredPredictions = 0;
            int groundTruths = FilterSet.testSetSize();
            this.header = header;
            rules = new ArrayList<>(map.keySet());

            switch (header) {
                case "Confidence":
                    rules.sort(IO.ruleComparatorBySC());
                    break;
                case "validPrecision":
                    rules.sort(IO.ruleComparatorByValidPrec());
                    break;
                case "localF1":
                    rules.sort(IO.ruleComparatorByLocalF1());
                    break;
                default:
                    System.err.println("Error: Unknown model selection strategy.");
                    System.exit(-1);
            }

            candidates = MultimapBuilder.hashKeys().arrayListValues().build();
            rules = rules.subList(0, Math.min(rules.size(), maxRuleSize));

            Set<Pair> visited = new HashSet<>();
            for (Rule rule : rules) {
                for (Pair pair : map.get(rule)) {
                    if(!visited.contains(pair)) {
                        if(!FilterSet.isKnown(pair)) {
                            filteredPredictions++;
                            if(FilterSet.inTestSet(pair)) {
                                testSupport++;
                            } else {
                                inferred++;
                            }
                        }
                        visited.add(pair);
                    }
                    candidates.put(pair, rule);
                }
            }

            known = candidates.keySet().size() - inferred;
            this.recall = groundTruths == 0 ? 0 : (double) testSupport / groundTruths;
            this.precision = filteredPredictions == 0 ? 0 : (double) testSupport / filteredPredictions;
        }

        public double getF1() {
            return (recall + precision) == 0 ? 0 : (2 * recall * precision) / (recall + precision);
        }

        public double getWeightedF1() {
            return (recall + precision) == 0 ? 0 : (1.7 * recall * precision) / (0.7 * precision + recall);
        }

        public List<Rule> getRules() {
            return rules;
        }

        public void report() {
            System.out.println(MessageFormat.format("# Model with {0} rules sorted by {1}: " +
                    "Recall = {2} | Precision = {3} | F1 = {4} | Inferred Predictions = {5} | Known Predictions = {6}"
                    , rules.size()
                    , header
                    , recall
                    , precision
                    , getF1()
                    , inferred
                    , known
            ));
        }
    }

}
