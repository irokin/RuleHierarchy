package uk.ac.ncl.Hierarchy;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import uk.ac.ncl.structure.Rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RuleHierarchy {
    BiMap<Rule, TreeNode> queryMap = HashBiMap.create();
    TreeNode root;

    public RuleHierarchy(Collection<Rule> rules) {
        root = new TreeNode(null, 0, null);
        Multimap<Integer, TreeNode> lengthMap = MultimapBuilder.treeKeys().hashSetValues().build();
        rules.forEach( rule -> lengthMap.put(rule.length(), new TreeNode(rule)));

        Integer[] lengths = lengthMap.keySet().toArray(new Integer[0]);
        for (Integer length : lengths) {
            if(length == 1)
                lengthMap.get(length).forEach(root::addChild);
            else {
                for (TreeNode child : lengthMap.get(length)) {
                    for (TreeNode parent : lengthMap.get(length - 1)) {
                        if(subsumption(parent.rule, child.rule)) {
                            parent.addChild(child);
                            child.parent = parent;
                            break;
                        }
                    }
                }
            }
        }
    }

    public List<Rule> initQueryMap() {
        List<Rule> list = new ArrayList<>();
        for (TreeNode child : root.children) {
            queryMap.put(child.rule, child);
            list.add(child.rule);
        }
        return list;
    }

    public synchronized List<Rule> getRuleChildren(Rule rule) {
        TreeNode node = queryMap.get(rule);
        try{
            if(node == null)
                throw new Exception("Rule is specialized before being logged into hierarchy query map.");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        List<Rule> rules = new ArrayList<>();
        for (TreeNode child : node.children) {
            queryMap.put(child.rule, child);
            rules.add(child.rule);
        }
        return rules;
    }

    public int size() {
        return size(root, 0);
    }

    public int size(Rule rule) {
        return size(queryMap.get(rule), 0);
    }

    private int size(TreeNode node, int sum) {
        if(node == null)
            return sum;
        if(node.children.isEmpty())
            return sum;
        for (TreeNode child : node.children) {
            sum = size(child, sum);
        }
        return node.children.size() + sum;
    }

    private boolean subsumption(Rule left, Rule right) {
        try {
            if(left.length() > right.length())
                throw new Exception("The left rule should be shorter than right rule in subsumption comparison.");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        for (int i = 0; i < left.bodyAtoms.size(); i++) {
            if(!left.bodyAtoms.get(i).equals(right.bodyAtoms.get(i)))
                return false;
        }
        return true;
    }

    static class TreeNode {
        TreeNode parent;
        List<TreeNode> children = new ArrayList<>();
        Rule rule;
        int length;

        TreeNode(Rule rule) {
            this.rule = rule;
            this.length = rule.length();
        }

        TreeNode(TreeNode parent, int length, Rule rule) {
            this.parent = parent;
            this.length = length;
            this.rule = rule;
        }

        public void addChild(TreeNode child) {
            children.add(child);
        }

        @Override
        public String toString() {
            return rule == null ? "top-rule" : rule.toString();
        }
    }

}
