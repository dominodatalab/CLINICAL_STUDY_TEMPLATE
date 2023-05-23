# Template Reporting Effort repository

This repo contains the folder structure to template a reporting effort making ADaM and TFL code for the Domino clinical trial demo

# Directory structure

The programming is created in a typical clinical trial folder structure, where the production (prod) and qc programs have independent directory trees.

Reporting effort level standard code (e.g. SAS macros) should be stored in the `share/macros` folder.

The global `domino.sas` autoexec progam is also included in the repository to appropriately set up the SAS environment. 

```
repo
│   domino.sas
|   init_datasets.py
├───prod
│   ├───adam
│   └───tfl
├───qc
│   ├───adam
│   │       compare.sas
│   └───tfl
└───share
    └───macros
```

# Setup

Create a new project, and then run `init_datasets.py` to create the appropriate analysis domino datasets (ADAM, TFL, ADAMQC and TFLQC). As well as import SDTM datasets from an existing project following the same naming convention (`XXXXX_SDTM` for `XXXXX_RE_YYYYY`).

# Naming convention

The programs follow a typical clinical trial naming convention, where the ADaM programs are named using the dataset name (e.g. ADSL.sas, etc.) and the TFL programs have a `t_` prefix to indicate tables, etc.

# QC programming and reporting

The QC programming is all in SAS, and there is a `compare.sas` program which uses SAS PROC COMPARE to create a summary report of all differences between the prod and qc datasets. This program also generates the `dominostats.json` files which Domino uses to display a dashboard in the jobs screen.

# Support

Programming was created by Veramed Ltd. on behalf of Domino Data Lab, Inc.
