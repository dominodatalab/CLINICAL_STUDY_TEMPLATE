/* cap input rows for the captured run */
options obs=100;

/* --------------------------------------------------------------------------
 * Caller setup for share/macros/tfl_metadata.sas. Upstream, __prog_name is
 * set by domino.sas and metadata.<__prog_name> is a Domino dataset imported
 * from the study's TFL_Metadata workbook. Here we set __prog_name and build
 * a small metadata.<__prog_name> WORK dataset so the macro has input.
 * ------------------------------------------------------------------------ */
%global __prog_name;
%let __prog_name = t_demo;

libname metadata (work);

/* one metadata row: mixed character + numeric columns. tfl_metadata turns
 * each column name into a same-named macro variable holding the row value. */
data metadata.t_demo;
  length DisplayName $40 DisplayTitle $60 Title1 $60 OrderNum 8 PageWidth 8;
  DisplayName  = "Table 14.3.1";
  DisplayTitle = "Adverse Events by Relationship";
  Title1       = "Treatment-Emergent Adverse Events";
  OrderNum     = 31;
  PageWidth    = 100;
  output;
run;
