/* cap input rows for the captured run */
options obs=100;

/* --------------------------------------------------------------------------
 * Stand-in for the SDTM.DM this program reads. Upstream, ADSL.sas reads
 * sdtm.dm from a Domino-mounted SDTM snapshot; here we provide a small
 * CDISC-Pilot-shaped DM (Xanomeline arms) as a WORK dataset so the program's
 * own logic runs unchanged. libname sdtm points at WORK.
 * ------------------------------------------------------------------------ */
libname sdtm (work);

data sdtm.dm;
  length studyid $8 usubjid $14 subjid $4 sex $1 age 8 arm actarm $20 country $3;
  infile datalines dsd truncover;
  input studyid $ subjid $ sex $ age arm $ actarm $ country $;
  usubjid = catx("-", studyid, subjid);
  datalines;
CDISC01,0001,F,61,Placebo,Placebo,USA
CDISC01,0002,M,74,Xanomeline High Dose,Xanomeline High Dose,USA
CDISC01,0003,F,68,Xanomeline Low Dose,Xanomeline Low Dose,USA
CDISC01,0004,M,58,Placebo,Placebo,USA
CDISC01,0005,F,79,Xanomeline High Dose,Xanomeline High Dose,USA
CDISC01,0006,M,63,Xanomeline Low Dose,Xanomeline Low Dose,USA
CDISC01,0007,F,71,Placebo,Placebo,USA
CDISC01,0008,M,66,Xanomeline High Dose,Xanomeline High Dose,USA
CDISC01,0009,F,84,Xanomeline Low Dose,Xanomeline Low Dose,USA
CDISC01,0010,M,55,Placebo,Placebo,USA
;
run;

/* ADSL is written to the ADAM library upstream; map it to WORK for the run */
libname adam (work);
