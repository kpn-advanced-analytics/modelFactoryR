
DROP TABLE IF EXISTS model_factory.model_varimp;
DROP TABLE IF EXISTS model_factory.model_summary;
DROP TABLE IF EXISTS model_factory.model_scores;
DROP TABLE IF EXISTS model_factory.model_backtesting;
DROP TABLE IF EXISTS model_factory.model_test_results;
DROP TABLE IF EXISTS model_factory.metadata_table;
DROP TABLE IF EXISTS model_factory.run_history;
DROP TABLE IF EXISTS model_factory.model_overview;

DROP SCHEMA IF EXISTS model_factory;

CREATE SCHEMA model_factory;


/*Stores Variable Importance in a run of:
 * Random foest models
 * Genereal Liniur Models
 * Logistic regression models
*/
DROP TABLE IF EXISTS model_factory.model_varimp;

CREATE TABLE model_factory.model_varimp
(
	session_id		VARCHAR(50),
	variable		VARCHAR(255),
	importance		NUMERIC(28,6),
	std				NUMERIC(28,6),
	coefficients	NUMERIC(28,6)
)
DISTRIBUTE BY HASH (variable);

/*Stores that summary od input data of the model for every run
 *
*/
DROP TABLE IF EXISTS model_factory.model_summary;

CREATE TABLE model_factory.model_summary
(
	session_id		VARCHAR(50),
	variable		VARCHAR(255),
	mean			NUMERIC(28,6),
	sd				NUMERIC(28,6),
	median			NUMERIC(28,6),
	min				NUMERIC(28,6),
	max				NUMERIC(28,6),
	n				INTEGER,
	n_na			INTEGER
)
DISTRIBUTE BY HASH (variable);


/*Stores the output scores of regression models
 *
*/
DROP TABLE IF EXISTS model_factory.model_scores;

CREATE TABLE model_factory.model_scores
(
	session_id		VARCHAR(50),
	id				VARCHAR(255),
	scores			NUMERIC(28,6),
	scores_class	VARCHAR(255)
)
DISTRIBUTE BY HASH (id);


/*Stores results from back testing time series models
 *
*/

DROP TABLE IF EXISTS model_factory.model_backtesting;

CREATE TABLE model_factory.model_backtesting
(
	session_id		VARCHAR(50),
	predicted_value	NUMERIC(28,6),
	actual_value	NUMERIC(28,6),
	period			TIMESTAMP
)
DISTRIBUTE BY HASH (predicted_value);

/*Stores results from back testing non-time series models
 * The data should be interprated as a ranked list order by scores in desending order
 * and the row represents a threshold , or cut off point.
 *
 * true_positives is the number of 1 lables are above the current row
 * false_positives is the number of 0 lables are above the current row
 * true_negatives is the number of 0 labeles below this row
 * false_negatives is teh number of 1 lables are below this row
*/

DROP TABLE IF EXISTS model_factory.model_test_results;

CREATE TABLE model_factory.model_test_results
(
	session_id		VARCHAR(50),
	score			NUMERIC(28,6),
	"label"			NUMERIC(28,6),
	population		NUMERIC(28,6),
	target_population NUMERIC(28,6),
	true_positives	INTEGER,
	false_positives	INTEGER,
	true_negatives	INTEGER,
	false_negatives	INTEGER
)
DISTRIBUTE BY HASH (score);


/*Stores key value data assosiated with a run
 * The group column is to allow data to be separated into multple groups when displying
*/

DROP TABLE IF EXISTS model_factory.metadata_table;

CREATE TABLE model_factory.metadata_table
(
	session_id		VARCHAR(50),
	session_key		VARCHAR(255),
	session_value	VARCHAR(255),
	session_group	VARCHAR(50)
)
DISTRIBUTE BY HASH (session_key);


/*Stores a record for every run
 * end_time is only filled if the run was complited
 * last_exported is only fill if and when the model's scores where exported to TD
*/
DROP TABLE IF EXISTS model_factory.run_history;

CREATE TABLE model_factory.run_history
(
	session_id		VARCHAR(50),
	user_id			VARCHAR(50),
	model_id		VARCHAR(50),
	start_time		TIMESTAMP,
	end_time		TIMESTAMP,
	last_exported	TIMESTAMP
)
DISTRIBUTE BY HASH (session_id);


/*Stores a description data about every model
 * if experimental = 1 model scores should be exported to TD under teh experimental model type
 * if producton = 1 model scores should be exported to TD under teh producton model type, this over rules the experimental flag
 * if both experimental <> 1 AND producton <> 1 the model scores should not be exported.
*/
DROP TABLE IF EXISTS model_factory.model_overview;

CREATE TABLE model_factory.model_overview
(
	model_id		VARCHAR(50),
	model_description TEXT,
	score_id_type	VARCHAR(255),
	threshold_value		VARCHAR(255),
	threshold_type	VARCHAR(255),
	experimental NUMERIC(1,0),
	production NUMERIC(1,0)
)
DISTRIBUTE BY HASH (model_id);


/*Stores the status os a column in the input data tables
* Is used to update multiple models at ones when introducing a new column
*/
DROP TABLE IF EXISTS model_factory.input_data_valid_columns;

CREATE TABLE model_factory.input_data_valid_columns
(
  schema_name	VARCHAR(63),
  table_name  VARCHAR(63),
  column_name VARCHAR(63),
  valid_column  TINYINT,
  test_column   TINYINT,
  column_info   TEXT
)
DISTRIBUTE BY HASH (column_name);


/*Stores RDS binary data of models
*/
DROP TABLE IF EXISTS model_factory.model_store;

CREATE DIMENSION TABLE model_factory.model_store (
  session_id		VARCHAR(50),
  model			    BYTEA
);


/*Create view for Tableau
*/

drop view if exists model_factory.tableau_input;

create view model_factory.tableau_input as (
select hist.session_id, hist.user_id, hist.model_id, ov.model_description, hist.start_time, hist.end_time,
varimp.variable,varimp.importance,varimp.coefficients,
summary.mean, summary.sd, summary.median, summary."min",summary."max", summary.n, summary.n_na,
test.population, test.target_population, test.true_positive_rate, test.false_positive_rate,
case when summary.mean is null then 0 else 1 end as summary_avaliable
from
	(select * from model_factory.run_history where  end_time>=current_date-30) hist
	left join model_factory.model_overview ov
		on hist.model_id=ov.model_id
	left join (select * from model_factory.model_varimp) varimp
		on hist.session_id=varimp.session_id
	left join (select * from model_factory.model_summary) summary
		on varimp.session_id=summary.session_id and varimp.variable=summary.variable
	left join (
	   select session_id, cast(population as decimal(10,2)) as population,
	   case when cast(population as decimal(10,2))=0 then 0 else max(target_population) end as target_population,
	   case when cast(population as decimal(10,2))=0 then 0 else
	   		cast(cast(max(true_positives) as float)/cast((max(true_positives)+max(false_negatives)) as float) as decimal(10,2))
	   			end as true_positive_rate,
	   case when cast(population as decimal(10,2))=0 then 0 else
	   		cast(cast(max(false_positives)as float)/cast((max(true_negatives)+max(false_positives)) as float) as decimal(10,2))
	   			end as false_positive_rate
	   from "model_factory"."model_test_results"
	   group by session_id, cast(population as decimal(10,2))
	   order by session_id, cast(population as decimal(10,2))) test
	     on hist.session_id=test.session_id);
