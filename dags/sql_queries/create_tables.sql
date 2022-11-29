CREATE TABLE IF NOT EXISTS public.heart_fact(
	"account_id" varchar,
    "age" int,
    "sex" varchar,
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

CREATE TABLE IF NOT EXISTS public.heart_disease_dim(
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

CREATE TABLE IF NOT EXISTS public.heart_disease_stage(
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

CREATE TABLE IF NOT EXISTS public.account_stage(
	"account_id" varchar,
    "age" int,
    "sex" varchar
);

CREATE TABLE IF NOT EXISTS public.account_dim(
	"account_id" varchar,
    "age" int,
    "sex" varchar
);
