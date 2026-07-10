/*****************************************************************************\
* Program              : ADVS.sas   (Domino CLINICAL_STUDY_TEMPLATE, prod/adam)
* Purpose              : Create ADaM ADVS dataset (ADSL merged with SDTM.VS)
*
* Bundle note: the Domino environment %include is provided by autoexec.sas
* instead (it supplies adam.adsl and sdtm.vs). The MERGE below is verbatim;
* the ADVS/ADLB/ADCM/ADMH programs in this repo share this same pattern.
\*****************************************************************************/

*********;
** Setup environment including libraries for this reporting effort;
** (provided by autoexec.sas in this bundle);
*********;

data adam.advs;
	merge adam.adsl sdtm.vs (in = v);
		by usubjid;
	if v;
run;

** surface the derived ADVS;
proc print data = adam.advs (obs = 20) label;
	title "ADVS derived from ADSL + SDTM.VS";
run;
