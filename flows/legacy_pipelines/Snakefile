from domino import Domino
from os import environ
from os.path import splitext

domino = Domino(
    f"{environ['DOMINO_PROJECT_OWNER']}/{environ['DOMINO_PROJECT_NAME']}"
)

def blocking_job(path):
    environments = {
        '.sas': "645163bc122db878f0012705"
    }
    file_name, file_ext = splitext(path)
    domino.job_start_blocking(
        command=path,
        environment_id=environments[file_ext]
    )

report: "/mnt/data/CDISC01_RE_INTERIM/snakemake.rst"

envvars:
    "DOMINO_USER_API_KEY",
    "DOMINO_API_HOST"

rule all:
    input:
        "/mnt/data/ADAM/adae.sas7bdat",
        "/mnt/data/ADAM/adcm.sas7bdat",
        "/mnt/data/ADAM/adlb.sas7bdat",
        "/mnt/data/ADAM/admh.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat",
        "/mnt/data/ADAM/advs.sas7bdat",
        "/mnt/data/TFL/t_ae_rel.sas7bdat",
        "/mnt/data/TFL/t_pop.sas7bdat",
        "/mnt/data/TFL/t_vscat.sas7bdat",
        "/mnt/data/METADATA/t_ae_rel.sas7bdat",
        "/mnt/data/METADATA/t_pop.sas7bdat",
        "/mnt/data/METADATA/t_vscat.sas7bdat"
        
rule metadata:
    input:
        "/mnt/code/import_metadata.sas",
        "/mnt/pvc-rev4-nfs/TFL_Metadata.xlsx"
    output:
        "/mnt/data/METADATA/t_ae_rel.sas7bdat",
        "/mnt/data/METADATA/t_pop.sas7bdat",
        "/mnt/data/METADATA/t_vscat.sas7bdat"
    log:
        "/mnt/artifacts/metadata.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule ADAE:
    input:
        "/mnt/code/prod/adam/ADAE.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/ae.sas7bdat",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/ex.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat"
    output:
        "/mnt/data/ADAM/adae.sas7bdat"
    log:
        "/mnt/artifacts/ADAE.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule ADCM:
    input:
        "/mnt/code/prod/adam/ADCM.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/cm.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat"
    output:
        "/mnt/data/ADAM/adcm.sas7bdat"
    log:
        "/mnt/artifacts/ADCM.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule ADLB:
    input:
        "/mnt/code/prod/adam/ADLB.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/lb.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat"
    output:
        "/mnt/data/ADAM/adlb.sas7bdat"
    log:
        "/mnt/artifacts/ADLB.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule ADMH:
    input:
        "/mnt/code/prod/adam/ADMH.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/mh.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat"
    output:
        "/mnt/data/ADAM/admh.sas7bdat"
    log:
        "/mnt/artifacts/ADMH.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule ADSL:
    input:
        "/mnt/code/prod/adam/ADSL.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/dm.sas7bdat"
    output:
        "/mnt/data/ADAM/adsl.sas7bdat"
    log:
        "/mnt/artifacts/ADSL.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule ADVS:
    input:
        "/mnt/code/prod/adam/ADVS.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/vs.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat"
    output:
        "/mnt/data/ADAM/advs.sas7bdat"
    log:
        "/mnt/artifacts/ADVS.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule t_ae_rel:
    input:
        "/mnt/code/prod/tfl/t_ae_rel.sas",
        "/mnt/data/METADATA/t_ae_rel.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat",
        "/mnt/data/ADAM/adae.sas7bdat"
    output:
        "/mnt/data/TFL/t_ae_rel.sas7bdat"
    log:
        "/mnt/artifacts/t_ae_rel.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule t_pop:
    input:
        "/mnt/code/prod/tfl/t_pop.sas",
        "/mnt/data/METADATA/t_pop.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat"
    output:
        "/mnt/data/TFL/t_pop.sas7bdat"
    log:
        "/mnt/artifacts/t_pop.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule t_vscat:
    input:
        "/mnt/code/prod/tfl/t_vscat.sas",
        "/mnt/data/METADATA/t_vscat.sas7bdat",
        "/mnt/data/ADAM/advs.sas7bdat"
    output:
        "/mnt/data/TFL/t_vscat.sas7bdat"
    log:
        "/mnt/artifacts/t_vscat.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_ADAE:
    input:
        "/mnt/code/qc/adam/qc_ADAE.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/ae.sas7bdat",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/ex.sas7bdat",
        "/mnt/data/ADAM/adae.sas7bdat",
        "/mnt/data/ADAMQC/adsl.sas7bdat"
    log:
        "/mnt/artifacts/qc_ADAE.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_ADCM:
    input:
        "/mnt/code/qc/adam/qc_ADCM.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/cm.sas7bdat",
        "/mnt/data/ADAM/adcm.sas7bdat",
        "/mnt/data/ADAMQC/adsl.sas7bdat"
    log:
        "/mnt/artifacts/qc_ADCM.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_ADLB:
    input:
        "/mnt/code/qc/adam/qc_ADLB.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/lb.sas7bdat",
        "/mnt/data/ADAM/adlb.sas7bdat",
        "/mnt/data/ADAMQC/adsl.sas7bdat"
    log:
        "/mnt/artifacts/qc_ADLB.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_ADMH:
    input:
        "/mnt/code/qc/adam/qc_ADMH.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/mh.sas7bdat",
        "/mnt/data/ADAM/admh.sas7bdat",
        "/mnt/data/ADAMQC/adsl.sas7bdat"
    log:
        "/mnt/artifacts/qc_ADMH.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_ADSL:
    input:
        "/mnt/code/qc/adam/qc_ADSL.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/dm.sas7bdat",
        "/mnt/data/ADAM/adsl.sas7bdat"
    log:
        "/mnt/artifacts/qc_ADSL.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_ADVS:
    input:
        "/mnt/code/qc/adam/qc_ADVS.sas",
        "/mnt/imported/data/snapshots/SDTMBLIND/MAY162023/vs.sas7bdat",
        "/mnt/data/ADAM/advs.sas7bdat",
        "/mnt/data/ADAMQC/adsl.sas7bdat"
    log:
        "/mnt/artifacts/qc_ADVS.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_t_ae_rel:
    input:
        "/mnt/code/qc/tfl/qc_t_ae_rel.sas",
        "/mnt/data/ADAM/adsl.sas7bdat",
        "/mnt/data/ADAM/adae.sas7bdat"
    log:
        "/mnt/artifacts/qc_t_ae_rel.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_t_pop:
    input:
        "/mnt/code/qc/tfl/qc_t_pop.sas",
        "/mnt/data/ADAM/adsl.sas7bdat"
    log:
        "/mnt/artifacts/qc_t_pop.log"
    run:
        run = blocking_job(input[0])
        print(run)

rule qc_t_vscat:
    input:
        "/mnt/code/qc/tfl/qc_t_vscat.sas",
        "/mnt/data/ADAM/advs.sas7bdat"
    log:
        "/mnt/artifacts/qc_t_vscat.log"
    run:
        run = blocking_job(input[0])
        print(run)
