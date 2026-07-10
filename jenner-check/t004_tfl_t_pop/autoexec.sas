/* cap input rows for the captured run */
options obs=100;

/* --------------------------------------------------------------------------
 * Environment stand-in for t_pop.sas. Upstream, %include domino.sas sets
 * __prog_name and points the ADAM / TFL / METADATA libnames at Domino
 * mounts, and %tfl_metadata reads metadata.<__prog_name> to create the
 * &DisplayName / &Title1 / &Footer* display macro variables. Here we supply
 * all of that as small WORK datasets so the program's SQL / macro / TRANSPOSE
 * / REPORT logic runs unchanged.
 * ------------------------------------------------------------------------ */
%global __prog_name;
%let __prog_name = t_pop;

libname adam (work);
libname tfl  (work);
libname metadata (work);

/* metadata.t_pop -- one row of display metadata; the column names become the
 * &DisplayName / &DisplayTitle / &Title1 / &Footer1-3 macro variables that
 * %tfl_metadata creates via call symput. */
data metadata.t_pop;
  length DisplayName $40 DisplayTitle $60 Title1 $60
         Footer1 $80 Footer2 $80 Footer3 $80;
  DisplayName  = "Table 14.1.1";
  DisplayTitle = "Summary of Populations";
  Title1       = "Age Group by Treatment (Safety Population)";
  Footer1      = "Percentages are based on the number of subjects in each treatment group.";
  Footer2      = "Reporting effort: CDISC01.";
  Footer3      = "Generated from ADSL.";
  output;
run;

/* adam.adsl -- subject level input with the ACTARM / AGE / SEX variables the
 * program derives TRTAN / AGEN / SEXN from (CDISC Pilot Xanomeline arms). */
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
CDISC01-0006,M,63,Xanomeline Low Dose
CDISC01-0007,F,71,Placebo
CDISC01-0008,M,66,Xanomeline High Dose
CDISC01-0009,F,84,Xanomeline Low Dose
CDISC01-0010,M,55,Placebo
CDISC01-0011,F,77,Xanomeline High Dose
CDISC01-0012,M,62,Xanomeline Low Dose
;
run;

/* --------------------------------------------------------------------------
 * The shared %tfl_metadata macro from share/macros/tfl_metadata.sas. Upstream
 * this is autoloaded via SASAUTOS; inlined here so the program resolves it.
 * Definition is verbatim from the repo.
 * ------------------------------------------------------------------------ */
%macro tfl_metadata();
	data metadata;
		set metadata.&__prog_name.;
	run;

	** create macro variables for all variable names;
	data _null_;
		set metadata;

		* numeric variables;
		array xxx{*} _numeric_;
		do i =1 to dim(xxx);
			call symput(vname(xxx[i]),xxx[i]);
		end;

		* character variables;
		array yyy{*} $ _character_;
		do i =1 to dim(yyy);
			call symput(vname(yyy[i]),yyy[i]);
		end;
	run; 
%mend;
