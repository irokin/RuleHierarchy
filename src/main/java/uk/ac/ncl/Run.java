package uk.ac.ncl;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.json.JSONObject;
import uk.ac.ncl.core.Engine;
import uk.ac.ncl.core.Evaluator;
import uk.ac.ncl.model.RH;
import uk.ac.ncl.utils.Helpers;
import uk.ac.ncl.utils.IO;
import uk.ac.ncl.validations.ValidRuleEvalEfficiency;
import uk.ac.ncl.validations.ValidRuleQuality;

import java.io.File;
import java.text.MessageFormat;

public class Run {
    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder("c").longOpt("config").hasArg().argName("FILE")
                .desc("Specify the location of the GPFL configuration file.").build());

        options.addOption(Option.builder("bg").longOpt("buildGraph")
                .desc("Build a Neo4j graph database from training/validation/test triple files.").build());

        options.addOption(Option.builder("sg").longOpt("splitGraph")
                .desc("Create training/test/validation splits from a graph database.").build());

        options.addOption(Option.builder("sf").longOpt("splitFiles")
                .desc("Create new training/test/validation splits from existing training/test/validation files.").build());

        options.addOption(Option.builder("r").longOpt("run")
                .desc("Learn, apply and evaluate rules for link prediction.").build());

        options.addOption(Option.builder("ver").longOpt("verbose").hasArg().argName("INTEGER")
                .desc("Set the verbosity level of the system.").build());

        options.addOption(Option.builder("v").longOpt("version")
                .desc("Current version.").build());

        options.addOption(Option.builder("e").longOpt("eval").hasArg().argName("FILE")
                .desc("Evaluate the provided prediction file.").build());

        options.addOption(Option.builder("ea").longOpt("evalAnyburl").hasArg().argName("FILE")
                .desc("Evaluate prediction files produced by AnyBURL.").build());

//        options.addOption(Option.builder("l").longOpt("learn")
//                .desc("Learn rules.").build());

        options.addOption(Option.builder("a").longOpt("apply")
                .desc("Apply rules.").build());

        options.addOption(Option.builder("sbg").longOpt("singleBuild").hasArg().argName("FILE")
                .desc("Bulid a Neo4j Graph Database from a single triple file.").build());

        options.addOption(Option.builder("or").longOpt("orderRule").hasArg().argName("FILE")
                .desc("Order the rule file by rule quality.").build());

        options.addOption(Option.builder("ert").longOpt("evalRuntime")
                .desc("Execute rule evaluation efficiency experiment.").build());

        options.addOption(Option.builder("p").longOpt("ruleQualityExp")
                .desc("Execute rule mining experiment.").build());

        options.addOption(Option.builder("st").longOpt("sampleTargets")
                .desc("Randomly sample targets.").build());

        options.addOption(Option.builder("ra").longOpt("ruleAnalysis")
                .desc("Perform rule quality analysis.").build());

        options.addOption(Option.builder("ov").longOpt("overfitting")
                .desc("Perform the overfitting analysis.").build());

        options.addOption(Option.builder("rha").longOpt("ruleHierarchyAnalysis")
                .desc("Rule Hierarchy Analysis").build());

        options.addOption(Option.builder("bu").longOpt("buildUnsolvable").hasArg().argName("FILE")
                .desc("Build datasets with unsolvable triples filtered.").build());

        options.addOption(Option.builder("uc").longOpt("unsolvableCheck").hasArg().argName("FILE")
                .desc("Check unsolvable triples in validation and test files.").build());

        options.addOption(Option.builder("h").longOpt("help").desc("Print help information.").build());
        String header = "GPFL is a probabilistic rule learner optimized to learn instantiated first-order logic rules from knowledge graphs. " +
                "For more information, please refer to https://github.com/irokin/GPFL";
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = (new DefaultParser()).parse(options, args);
            if (cmd.hasOption("h"))
                formatter.printHelp("Graph Path Feature Learning System (GPFL)", header, options, "", false);

            if (cmd.hasOption("v"))
                System.out.println(MessageFormat.format("# GPFL System Version: {0} | Date: {1}"
                        , Settings.VERSION
                        , Settings.DATE)
                );

            if (cmd.hasOption("sbg"))
                Engine.buildGraphSingleFile(cmd.getOptionValue("sbg"));

            if (cmd.hasOption("ver"))
                Settings.VERBOSITY = Integer.parseInt(cmd.getOptionValue("ver"));

            if (cmd.hasOption("e"))
                Evaluator.evalGPFL(cmd.getOptionValue("e"));

            if (cmd.hasOption("ea"))
                Evaluator.evalAnyBURL(cmd.getOptionValue("ea"));

            if (cmd.hasOption("or"))
                IO.orderRules(new File(cmd.getOptionValue("or")));

            if (cmd.hasOption("bu"))
                IO.buildUnsolvableFiles(cmd.getOptionValue("bu"));

            if (cmd.hasOption("uc"))
                IO.detectUnsolvable(cmd.getOptionValue("uc"));

            if(cmd.hasOption("c")) {
                File config = new File(cmd.getOptionValue("c"));
                JSONObject jsonArgs = Helpers.buildJSONObject(config);
                String home = jsonArgs.getString("home");

                if (cmd.hasOption("ra")) {
                    ValidRuleQuality eval = new ValidRuleQuality(config, "rule_analysis");
                    eval.analyzeRuleComposition();
                }

                if (cmd.hasOption("ert")) {
                    ValidRuleEvalEfficiency eval = new ValidRuleEvalEfficiency(config, "runtime");
                    eval.eval();
                }

                if (cmd.hasOption("p")) {
                    ValidRuleQuality eval = new ValidRuleQuality(config, "rule_quality");
                    eval.evalPrecision();
                }

                if (cmd.hasOption("bg"))
                    Engine.buildGraph(home).shutdown();

                if (cmd.hasOption("sg"))
                    Engine.createRandomSplitsFromGraph(config).shutdown();

                if (cmd.hasOption("sf"))
                    Engine.createRandomSplitsFromFiles(config);

                if (cmd.hasOption("st")) {
                    Engine.selectTargets(config);
                }

                if (cmd.hasOption("ov")) {
                    ValidRuleQuality eval = new ValidRuleQuality(config, "overfit_analysis");
                    eval.overfittingEval();
                }

                if (cmd.hasOption("r")) {
                    RH system = new RH(config, "log");
                    system.run();
                }

                if (cmd.hasOption("rha")) {
                    IO.analyzeForRH(config);
                }

                if (cmd.hasOption("l")) {
                    RH system = new RH(config, "learn_log");
                    system.learn();
                }

                if (cmd.hasOption("a")) {
                    RH system = new RH(config, "apply_log");
                    system.apply();
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
