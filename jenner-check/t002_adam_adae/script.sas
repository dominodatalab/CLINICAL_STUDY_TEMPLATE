/*****************************************************************************\
* Program              : ADAE.sas   (Domino CLINICAL_STUDY_TEMPLATE, prod/adam)
* Purpose              : Create ADaM ADAE dataset (AE merged with EX)
*
* Bundle note: the Domino environment %include is provided by autoexec.sas
* instead (it supplies adam.adsl, sdtm.ae, sdtm.ex). The DATA/PROC SORT logic
* below is verbatim from the upstream program.
\*****************************************************************************/

*********;
** Setup environment including libraries for this reporting effort;
** (provided by autoexec.sas in this bundle);
*********;

data adae;
	merge adam.adsl sdtm.ae (in = ae);
		by usubjid;
	if ae;
	if 1 <= aestdy < 13 then visitnum = 3;
	else if 13 <= aestdy < 161 then visitnum = 4;
	else if 162 <= aestdy then visitnum = 12;
run;

proc sort data = adae out = adae_s;
	by usubjid visitnum;
run;

data adam.adae;
	merge adae_s (in = ae) sdtm.ex;
	by usubjid visitnum;
	if ae;
run;

** surface the derived ADAE;
proc print data = adam.adae (obs = 20) label;
	title "ADAE derived from SDTM.AE / SDTM.EX";
run;
