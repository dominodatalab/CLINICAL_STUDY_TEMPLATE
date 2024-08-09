import os
from flytekit import workflow
from flytekit.types.file import FlyteFile
from utils.adam import create_adam_data
from utils.tfl import create_tfl_report
from typing import TypeVar, NamedTuple

tfl_outputs = NamedTuple("tfl_outputs", t_ae_rel=FlyteFile[TypeVar("pdf")], t_vscat=FlyteFile[TypeVar("pdf")])

@workflow
def ADaM_TFL(sdtm_data_path: str) -> tfl_outputs:
    """
    This script mocks a sample clinical trial using Domino Flows. 

    The input to this flow is the path to your SDTM data. You can point this to either your SDTM-BLIND dataset or your SDTM-UNBLIND dataset. The output to this flow are a series of TFL reports.

    To the run the workflow remotely, execute the following code in your terminal:
    
    pyflyte run --remote workflow.py ADaM_TFL --sdtm_data_path /mnt/code/data/sdtm-blind

    :param sdtm_data_path: The root directory of your SDTM dataset
    :return: A list of PDF files containing the TFL reports
    """
    # Create task that generates ADSL dataset. This will run a unique Domino job and return its outputs.
    adsl = create_adam_data(
        name="ADSL", 
        command="prod/adsl.sas",
        environment="SAS Analytics Pro",
        hardware_tier= "Small", # Optional parameter. If not set, then the default for the project will be used.
        sdtm_data_path=sdtm_data_path # Note this this is simply the input value taken in from the command line argument
    )
    # Create task that generates ADAE dataset. 
    adae = create_adam_data(
        name="ADAE", 
        command="prod/adae.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl] # Note how this is the output from the previous task
    )
    # Create task that generates ADVS dataset. 
    advs = create_adam_data(
        name="ADVS", 
        command="prod/advs.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl, adae]
    )
    # Create task that generates TFL report from ADAE dataset.
    t_ae_rel = create_tfl_report(
        name="T_AE_REL", 
        command="prod/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[adae]
    )
    # Create task that generates TFL report from ADVS dataset
    t_vscat = create_tfl_report(
        name="T_VSCAT", 
        command="prod/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[advs]
    )
    return tfl_outputs(t_ae_rel=t_ae_rel, t_vscat=t_vscat)