CREATE TABLE IF NOT EXISTS heart_analysis.heart_fact(
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
    PRIMARY KEY("account_id")
);

CREATE TABLE IF NOT EXISTS heart_analysis.heart_disease_dim(
	"account_id" varchar,
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
    PRIMARY KEY("account_id")
);

CREATE TABLE IF NOT EXISTS heart_analysis.heart_disease_stage(
	"account_id" varchar,
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
    "target" int
);

CREATE TABLE IF NOT EXISTS heart_analysis.account_stage(
	"account_id" varchar,
    "sex" int,
    "age" int
);

CREATE TABLE IF NOT EXISTS heart_analysis.account_dim(
	"account_id" varchar,
    "sex" int,
    "age" int,
    PRIMARY KEY("account_id")
);
