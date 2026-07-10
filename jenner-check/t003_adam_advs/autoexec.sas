/* cap input rows for the captured run */
options obs=100;

/* --------------------------------------------------------------------------
 * Stand-ins for the inputs ADVS.sas reads: adam.adsl (subject level) and
 * sdtm.vs (vital signs). Upstream these come from the ADAM library and a
 * Domino-mounted SDTM snapshot; here they are small CDISC-shaped WORK
 * datasets so the merge runs unchanged.
 * ------------------------------------------------------------------------ */
libname sdtm (work);
libname adam (work);

data adam.adsl;
  length usubjid $14 sex $1 age 8 actarm $20;
  infile datalines dsd truncover;
  input usubjid $ sex $ age actarm $;
  datalines;
CDISC01-0001,F,61,Placebo
CDISC01-0002,M,74,Xanomeline High Dose
CDISC01-0003,F,68,Xanomeline Low Dose
CDISC01-0004,M,58,Placebo
;
run;

/* SDTM.VS -- vital signs; SYSBP/DIABP/PULSE across visits, with the
 * VSTESTCD / VSSTRESN / VISITNUM shape the downstream TFL programs expect. */
data sdtm.vs;
  length usubjid $14 vstestcd $6 vstest $24 vsstresn 8 visitnum 8;
  infile datalines dsd truncover;
  input usubjid $ vstestcd $ vstest $ vsstresn visitnum;
  datalines;
CDISC01-0001,SYSBP,Systolic Blood Pressure,142,3
CDISC01-0001,DIABP,Diastolic Blood Pressure,88,3
CDISC01-0001,PULSE,Pulse Rate,72,3
CDISC01-0002,SYSBP,Systolic Blood Pressure,120,4
CDISC01-0002,DIABP,Diastolic Blood Pressure,55,4
CDISC01-0002,PULSE,Pulse Rate,105,4
CDISC01-0003,SYSBP,Systolic Blood Pressure,88,3
CDISC01-0003,PULSE,Pulse Rate,58,3
CDISC01-0004,SYSBP,Systolic Blood Pressure,135,12
CDISC01-0004,DIABP,Diastolic Blood Pressure,80,12
;
run;
