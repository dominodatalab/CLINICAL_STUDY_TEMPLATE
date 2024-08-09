from flytekit import workflow
from flytekit.types.file import FlyteFile
from typing import TypeVar, NamedTuple
from flytekitplugins.domino.helpers import Input, Output, run_domino_job_task
from flytekitplugins.domino.task import DominoJobConfig, DominoJobTask, GitRef, EnvironmentRevisionSpecification, EnvironmentRevisionType, DatasetSnapshot

# pyflyte run --remote flow_3.py ADaM_only_QC --sdtm_dataset_snapshot /mnt/imported/data/snapshots/SDTMBLIND/35

@workflow
def ADaM_only_QC(sdtm_dataset_snapshot: str): # -> FlyteFile[TypeVar("sas7bdat")]:

    #PROD 
    adsl = run_domino_job_task(
        flyte_task_name="Create ADSL Dataset",
        command="prod/adam_flows/ADSL.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot)],
        output_specs=[Output(name="adsl", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    ) 

    #PROD 
    adae = run_domino_job_task(
        flyte_task_name="Create ADAE Dataset",
        command="prod/adam_flows/ADAE.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="adae", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #PROD 
    adcm = run_domino_job_task(
        flyte_task_name="Create ADCM Dataset",
        command="prod/adam_flows/ADCM.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="adcm", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #PROD 
    adlb = run_domino_job_task(
        flyte_task_name="Create ADLB Dataset",
        command="prod/adam_flows/ADLB.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="adlb", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #PROD 
    admh = run_domino_job_task(
        flyte_task_name="Create ADMH Dataset",
        command="prod/adam_flows/ADMH.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="admh", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #PROD 
    advs = run_domino_job_task(
        flyte_task_name="Create ADVS Dataset",
        command="prod/adam_flows/ADVS.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="advs", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #QC 
    qc_adsl = run_domino_job_task(
        flyte_task_name="Create QC ADSL Dataset",
        command="qc/adam_flows/qc_ADSL.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot)],
        output_specs=[Output(name="qc_adsl", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    ) 
 
    #QC 
    qc_adae = run_domino_job_task(
        flyte_task_name="Create QC ADAE Dataset",
        command="qc/adam_flows/qc_ADAE.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="qc_adsl", type=FlyteFile[TypeVar("sas7bdat")], value=qc_adsl["qc_adsl"])],
        output_specs=[Output(name="qc_adae", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #QC 
    qc_adcm = run_domino_job_task(
        flyte_task_name="Create QC ADCM Dataset",
        command="qc/adam_flows/qc_ADCM.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="qc_adsl", type=FlyteFile[TypeVar("sas7bdat")], value=qc_adsl["qc_adsl"])],
        output_specs=[Output(name="qc_adcm", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #QC 
    qc_adlb = run_domino_job_task(
        flyte_task_name="Create QC ADLB Dataset",
        command="qc/adam_flows/qc_ADLB.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="qc_adsl", type=FlyteFile[TypeVar("sas7bdat")], value=qc_adsl["qc_adsl"])],
        output_specs=[Output(name="qc_adlb", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #QC 
    qc_admh = run_domino_job_task(
        flyte_task_name="Create QC ADMH Dataset",
        command="qc/adam_flows/qc_ADMH.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="qc_adsl", type=FlyteFile[TypeVar("sas7bdat")], value=qc_adsl["qc_adsl"])],
        output_specs=[Output(name="qc_admh", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    #QC 
    qc_advs = run_domino_job_task(
        flyte_task_name="Create QC ADVS Dataset",
        command="qc/adam_flows/qc_ADVS.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="qc_adsl", type=FlyteFile[TypeVar("sas7bdat")], value=qc_adsl["qc_adsl"])],
        output_specs=[Output(name="qc_advs", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    # Output from the task above will be used in the next step

    #return #final_outputs