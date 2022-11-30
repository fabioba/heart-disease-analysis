"""
This module contains the SqlQueries which is responsible for running insert statements

Author: Fabio Barbazza
Date: Nov, 2022
"""

class SqlQueries:
    """
    A class used to perform inserting statements

    Attributes:
    - insert_account(str)
    """
    insert_account_dimension = ("""
        SELECT
            distinct 
            account_stage.account_id,
            account_stage.age,
            account_stage.sex
        FROM 
            heart_analysis.account_stage as account_stage
        LEFT JOIN 
            heart_analysis.account_dim as account_dim
        ON 
            account_stage.account_id = account_dim.account_id
        WHERE
            account_dim.account_id is NULL
    """)

    insert_heart_disease_dimension = ("""
        SELECT
            distinct 
            heart_disease_stage.account_id,
            heart_disease_stage.cp,
            heart_disease_stage.trestbps,
            heart_disease_stage.chol,
            heart_disease_stage.fbs,
            heart_disease_stage.restecg,
            heart_disease_stage.thalach,
            heart_disease_stage.exang,
            heart_disease_stage.oldpeak,
            heart_disease_stage.slope,
            heart_disease_stage.ca,
            heart_disease_stage.thal,
            heart_disease_stage.target
        FROM 
            heart_analysis.heart_disease_stage as heart_disease_stage
        LEFT JOIN 
            heart_analysis.heart_disease_dim as heart_disease_dim
        ON 
            heart_disease_stage.account_id = heart_disease_dim.account_id
        WHERE
            heart_disease_dim.account_id is NULL
    """)

    insert_heart_fact = ("""
        SELECT
            distinct 
            account_dim.account_id,
            account_dim.age,
            account_dim.sex,
            heart_disease_dim.cp,
            heart_disease_dim.trestbps,
            heart_disease_dim.chol,
            heart_disease_dim.fbs,
            heart_disease_dim.restecg,
            heart_disease_dim.thalach,
            heart_disease_dim.exang,
            heart_disease_dim.oldpeak,
            heart_disease_dim.slope,
            heart_disease_dim.ca,
            heart_disease_dim.thal,
            heart_disease_dim.target
        FROM 
            heart_analysis.account_dim as account_dim
        LEFT JOIN 
            heart_analysis.heart_disease_dim as heart_disease_dim
        ON 
            account_dim.account_id = heart_disease_dim.account_id
        LEFT JOIN 
            heart_fact
        ON
            account_dim.account_id = heart_fact.account_id
        WHERE 
            heart_fact.account_id is NULL
    """)