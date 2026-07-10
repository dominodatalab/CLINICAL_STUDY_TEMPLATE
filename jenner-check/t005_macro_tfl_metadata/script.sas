/*****************************************************************************\
* Exercises: share/macros/tfl_metadata.sas  (Domino CLINICAL_STUDY_TEMPLATE)
*
* %tfl_metadata reads metadata.<__prog_name>, then loops the row's numeric and
* character columns with _numeric_ / _character_ arrays and call symput() to
* create a same-named macro variable for every column. This bundle is a small
* caller (upstream the macro is autoloaded via SASAUTOS and invoked from the
* TFL programs, e.g. t_pop.sas / t_vscat.sas). The macro definition itself is
* verbatim from share/macros/tfl_metadata.sas, inlined below.
\*****************************************************************************/

*********;
** Setup (provided by autoexec.sas: __prog_name + metadata.t_demo);
*********;

/* --- verbatim from share/macros/tfl_metadata.sas --- */
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

** invoke the shared macro;
%tfl_metadata;

** confirm the macro variables the macro created from the metadata row;
%put NOTE: DisplayName  = &DisplayName.;
%put NOTE: DisplayTitle = &DisplayTitle.;
%put NOTE: Title1       = &Title1.;
%put NOTE: OrderNum     = &OrderNum.;
%put NOTE: PageWidth    = &PageWidth.;

** and show them in a small dataset for a visible listing;
data macro_vars_check;
	length name $16 value $60;
	name = "DisplayName";  value = "&DisplayName.";  output;
	name = "DisplayTitle"; value = "&DisplayTitle."; output;
	name = "Title1";       value = "&Title1.";       output;
	name = "OrderNum";     value = "&OrderNum.";      output;
	name = "PageWidth";    value = "&PageWidth.";     output;
run;

proc print data = macro_vars_check label noobs;
	title "Macro variables created by tfl_metadata from the metadata row";
run;
