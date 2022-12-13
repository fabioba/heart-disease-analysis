CREATE TABLE IF NOT EXISTS heart_analysis.heart_fact_cleaned(
	"account_id" varchar,
    "age" int,
    "sex" int,
    "cp" int,
    "trestbps" int,
    "chol" int,
    "fbs" int,
    "restecg" int,
    "thalach" int,
    "exang" int,
    "oldpeak" float,
    "slope" int,
    "ca" int,
    "thal" int,
    "target" int,
    "pipeline_run" varchar,
    PRIMARY KEY("account_id","pipeline_run")
);

CREATE TABLE IF NOT EXISTS heart_analysis.heart_x_train(
    "age" int,
    "sex" int,
    "cp" int,
    "trestbps" int,
    "chol" int,
    "fbs" int,
    "restecg" int,
    "thalach" int,
    "exang" int,
    "oldpeak" float,
    "slope" int,
    "ca" int,
    "thal" int,
    "pipeline_run" varchar
);

CREATE TABLE IF NOT EXISTS heart_analysis.heart_x_test(
    "age" int,
    "sex" int,
    "cp" int,
    "trestbps" int,
    "chol" int,
    "fbs" int,
    "restecg" int,
    "thalach" int,
    "exang" int,
    "oldpeak" float,
    "slope" int,
    "ca" int,
    "thal" int,
    "pipeline_run" varchar
);

CREATE TABLE IF NOT EXISTS heart_analysis.heart_y_train(
    "target" int,
    "pipeline_run" varchar
);

CREATE TABLE IF NOT EXISTS heart_analysis.heart_y_test(
    "target" int,
    "pipeline_run" varchar
);
