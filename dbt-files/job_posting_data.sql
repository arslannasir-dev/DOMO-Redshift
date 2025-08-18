select
    JP.CREATION_DATE,
    JP.POSTING_EFFECTIVE_DATE,
    C."name" as "Company Name",
    JP.job_posting_id, H.HIRE_ID,JP.BOUNTY,
    ROUND(
        JP.BOUNTY *
        (
            CASE
                WHEN JP.CURRENCY_CODE = 'USD' THEN 1
                ELSE (
                    SELECT CR.VALUE
                    FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
                    WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(JP.CURRENCY_CODE))
                      AND CR.TARGET_CURRENCY_CODE = 'USD'
                      AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
                    ORDER BY CR.EFFECTIVE_DATE DESC
                    LIMIT 1
                )
            END
        )
    , 2
    ) AS BOUNTY_USD_CONVERTED,
     H.FEE_AMOUNT,
   	(case when CJP.ID IS NULL then (ROUND((
        JP.SALARY_MAXIMUM_RANGE * ((case when JP.CURRENCY_CODE = 'USD' then 1 else
                                      (case when JP.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(JP.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end) end))
        ), 2)) else NULL end) AS SALARY_MAXIMUM_RANGE_USD_CONVERTED,
        JP.salary_maximum_range,
    	(case when CJP.ID IS NULL then (ROUND((
        JP.SALARY_MINIMUM_RANGE * ((case when JP.CURRENCY_CODE = 'USD' then 1 else
                                      (case when JP.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(JP.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end) end))
        ), 2)) else NULL end) AS SALARY_MINIMUM_RANGE_USD_CONVERTED,
        JP.salary_minimum_range
FROM {{ source('bounty_jobs', 'job_posting') }} JP
left join {{ source('bounty_jobs', 'contract_job_posting') }} CJP on CJP.job_posting_id = JP.job_posting_id
left join {{ source('bounty_jobs_enhanced'.'bountyjobs_sql_hire') }} h on JP.job_posting_id = H.job_posting_id
left join {{ source('bounty_jobs', 'company') }} C on JP.company_id = C.company_id