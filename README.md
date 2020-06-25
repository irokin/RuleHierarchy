# Rule Hierarchy Framework

## Overview
<p align="center">
    <img src="https://www.dropbox.com/s/14zqhcozpf0td2p/hier_screenshot.png?raw=1">
</p>

This repository contains the codebase and datasets as described in [paper](). We introduce the Rule Hierarchy Framework (RHF) that efficiently organizes the rules mined by walk-based (bottom-up) logical rule learners into a proper rule hierarchy. In this repository, we augment the [GPFL system](https://github.com/irokin/GPFL) with the adaptation of RHF and the extensions of two hierarchical pruning methods (HPMs) that utilize the generated rule hierarchy to prune irrelevant rules. The Figure above shows an incomplete rule hierarchy where rules are linked by subsumption relations.   

## Requirements
- Java >= 1.8
- Gradle >= 5.6.4

## Getting Started
To verify the system, run:
```shell script
gradle run --args="-c data/UWCSE/config.json -r"
```

which executes:
- Rule Learning: learn rules for target predicates. The learned rules in this example can be found at `data/UWCSE/i3c3/rules.txt`.
- Rule Application: Apply the learned rules to infer new facts. The inferred facts can be found at `data/UWCSE/i3c3/predictions.txt`, and in file `data/UWCSE/i3c3/verifications.txt` you can find the explanations for both inferred and known facts.
- System Evaluation: Evaluate the learned rules in filtered hits@n and MRR. Detailed report can be found at `data/UWCSE/i3c3/eval_log.txt`.

To turn on/off the prior and post pruning and tune the prior threshold, please change the configuration file `data/UWCSE/config.json` by setting:
- `use_prior_prune`: true/false. Enable the execution of the preparations that are related to the prior pruning.
- `prior_th`: set the prior threshold.
- `use_post_prune`: true/false. Enable the post pruning.

For details about other settings, please refer to [GPFL Codebase](https://github.com/irokin/GPFL).

## Reproduce Experiment Results
Please download the datasets from [here](https://www.dropbox.com/s/p5rseuzntkhos7f/data.zip?dl=1), and unzip the compressed files into the `data` folder. We recommend running the experiments with at least 8 CPU cores and 64GB RAM for completion in reasonable time.

Over datasets `{FB15K-237-LV, WN18RR-LV, NELL995-LV, OpenBioLink}`, one can evaluate the effectiveness of the prior and post pruning by tuning `prior_th` and `use_post_prune` and keep all other settings unchanged in their individual configuration file, and run:
```shell script
gradle run --args="-c data/[dataset]/config.json -r"
``` 

