/* cap input rows for the captured run */
options obs=100;

/* --------------------------------------------------------------------------
 * Stand-ins for the inputs ADAE.sas reads. Upstream these come from a
 * Domino-mounted SDTM snapshot (sdtm.ae, sdtm.ex) and the ADAM library
 * (adam.adsl). Here we build small CDISC-shaped WORK datasets so the
 * program's merge / derivation logic runs unchanged.
 * ------------------------------------------------------------------------ */
libname sdtm (work);
libname adam (work);

/* ADSL (subject level) -- as produced by the ADSL program in this repo */
data adam.adsl;
  length usubjid $14 sex $1 age 8 actarm $20;
  infile datalines dsd truncover;
  input usubjid $ sex $ age actarm $;
  datalines;
CDISC01-0001,F,61,Placebo
CDISC01-0002,M,74,Xanomeline High Dose
CDISC01-0003,F,68,Xanomeline Low Dose
CDISC01-0004,M,58,Placebo
CDISC01-0005,F,79,Xanomeline High Dose
;
run;

/* SDTM.AE -- adverse events, one or more per subject, spanning study days
 * that exercise all three VISITNUM branches (1-12, 13-161, 162+). */
data sdtm.ae;
  length usubjid $14 aeterm $20 aestdy 8;
  infile datalines dsd truncover;
  input usubjid $ aeterm $ aestdy;
  datalines;
CDISC01-0001,HEADACHE,3
CDISC01-0001,NAUSEA,45
CDISC01-0002,DIZZINESS,10
CDISC01-0002,RASH,170
CDISC01-0003,FATIGUE,120
CDISC01-0004,INSOMNIA,200
CDISC01-0005,HEADACHE,2
;
run;

/* SDTM.EX -- exposure records keyed by usubjid + visitnum */
data sdtm.ex;
  length usubjid $14 extrt $20 visitnum 8;
  infile datalines dsd truncover;
  input usubjid $ extrt $ visitnum;
  datalines;
CDISC01-0001,PLACEBO,3
CDISC01-0001,PLACEBO,4
CDISC01-0002,XANOMELINE,3
CDISC01-0002,XANOMELINE,12
CDISC01-0003,XANOMELINE,4
CDISC01-0004,PLACEBO,12
CDISC01-0005,XANOMELINE,3
;
run;
