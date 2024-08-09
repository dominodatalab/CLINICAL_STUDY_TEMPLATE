from flytekit import workflow
from flytekit.types.file import FlyteFile
from typing import TypeVar, NamedTuple
from flytekitplugins.domino.helpers import Input, Output, run_domino_job_task
from flytekitplugins.domino.task import DominoJobConfig, DominoJobTask, GitRef, EnvironmentRevisionSpecification, EnvironmentRevisionType, DatasetSnapshot

# Command to run this Flow. There are two Flow input parameters. One for the SDTM Dataset snapshot and one for the METADATA dataset snapshot.
# pyflyte run --remote flow_2.py ADaM_TFL --sdtm_dataset_snapshot /mnt/imported/data/snapshots/SDTMBLIND/35 --metadata_snapshot /mnt/data/snapshots/METADATA/1 

@workflow
def ADaM_TFL(sdtm_dataset_snapshot: str, metadata_snapshot: str): 

    #Crete ADSL dataset. The only input is the SDTM Dataset. 
    adsl = run_domino_job_task(
        flyte_task_name="Create ADSL Dataset",
        command="prod/adam_flows/ADSL.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot)],
        output_specs=[Output(name="adsl", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
       
    ) 

    #Crete ADAE dataset. This has two inputs, the SDTM Dataset and the output from the previous task i.e. ADSL. 
    adae = run_domino_job_task(
        flyte_task_name="Create ADAE Dataset",
        command="prod/adam_flows/ADAE.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="adae", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )
    
    adcm = run_domino_job_task(
        flyte_task_name="Create ADCM Dataset",
        command="prod/adam_flows/ADCM.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="adcm", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    adlb = run_domino_job_task(
        flyte_task_name="Create ADLB Dataset",
        command="prod/adam_flows/ADLB.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="adlb", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    admh = run_domino_job_task(
        flyte_task_name="Create ADMH Dataset",
        command="prod/adam_flows/ADMH.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="admh", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    advs = run_domino_job_task(
        flyte_task_name="Create ADVS Dataset",
        command="prod/adam_flows/ADVS.sas",
        inputs=[Input(name="sdtm_dataset_snapshot", type=str, value=sdtm_dataset_snapshot),
                Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"])],
        output_specs=[Output(name="advs", type=FlyteFile[TypeVar("sas7bdat")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    t_pop = run_domino_job_task(
        flyte_task_name="Create T_POP Report",
        command="prod/tfl_flows/t_pop.sas",
        inputs=[Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"]),
                Input(name="metadata_snapshot", type=str, value=metadata_snapshot)],
        output_specs=[Output(name="t_pop", type=FlyteFile[TypeVar("pdf")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    t_ae_rel = run_domino_job_task(
        flyte_task_name="Create T_AE_REL Report",
        command="prod/tfl_flows/t_ae_rel.sas",
        inputs=[Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl["adsl"]),
                Input(name="adae", type=FlyteFile[TypeVar("sas7bdat")], value=adae["adae"]),
                Input(name="metadata_snapshot", type=str, value=metadata_snapshot)],
        output_specs=[Output(name="t_ae_rel", type=FlyteFile[TypeVar("pdf")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )

    t_vscat = run_domino_job_task(
        flyte_task_name="Create T_VSCAT Report",
        command="prod/tfl_flows/t_vscat.sas",
        inputs=[Input(name="advs", type=FlyteFile[TypeVar("sas7bdat")], value=advs["advs"]),
                Input(name="metadata_snapshot", type=str, value=metadata_snapshot)],
        output_specs=[Output(name="t_vscat", type=FlyteFile[TypeVar("pdf")])],
        use_project_defaults_for_omitted=True,
        environment_name="SAS Analytics Pro",
    )


    # Output from the task above will be used in the next step

    #return #final_outputs