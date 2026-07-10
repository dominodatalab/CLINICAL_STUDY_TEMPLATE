/*****************************************************************************\
* Program              : ADSL.sas   (Domino CLINICAL_STUDY_TEMPLATE, prod/adam)
* Purpose              : Create ADaM ADSL dataset from SDTM.DM
*
* Bundle note: the only change from the upstream program is that the Domino
* environment %include (which points libnames at mounted SDTM/ADAM snapshots)
* is provided by autoexec.sas instead. The DATA-step logic below is verbatim.
\*****************************************************************************/

*********;
** Setup environment including libraries for this reporting effort;
** (provided by autoexec.sas in this bundle);
*********;

data adam.adsl;
	set sdtm.dm;
run;

** show the derived ADSL so the run has a visible result;
proc print data = adam.adsl (obs = 10) label;
	title "ADSL derived from SDTM.DM";
run;
