-- Data Source : BountyJobs | SQL | AWARD_DIRECT : https://bountyjobs.domo.com/datasources/373381d4-e8cb-4308-9906-67ebdd9ad8b5/details/settings
{{ config(
    materialized='table',
    tags=['Conversion', 'slow'],
    sort='HIRE_MESSAGE_ID',
    dist='HIRE_MESSAGE_ID'
) }}
SELECT 
    HM.HIRE_MESSAGE_ID,
    S.SUBMISSION_ID,
    HMM.SENDING_PERSON_ID,
    HMM.RECEIVING_PERSON_ID,
    Coalesce(HRT.DESCRIPTION,'Pending') AS RESPONSE_TYPE,
    (SELECT MT.BODY_TEXT FROM {{ source('bounty_jobs', 'message_text') }} MT WHERE MT.ID = HMM.MESSAGE_ID) AS MESSAGE,
    HMM.CREATION_DATE AS REQUEST_DATE,
    HRM.SENDING_PERSON_ID as HRM_SENDING_PERSON_ID ,
    HRM.CREATION_DATE AS RESPONSE_DATE,
    DATE(HM.HIRE_DATE) AS HIRE_DATE,
    HM.SALARY,
	  ((ROUND((HM.SALARY * (
    case when HM.CURRENCY_CODE = 'USD' then 1 else
    (case when HM.HIRE_DATE > CURRENT_DATE then 
    (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
            WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
              AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
              AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
            ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  else
	  (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  end
          )
	  end)), 2))) AS SALARY_USD_CONVERTED,
	  JP.BOUNTY,
	  ROUND((BOUNTY * (
      case when HM.CURRENCY_CODE = 'USD' then 1 else
      (case when HM.HIRE_DATE > CURRENT_DATE then 
      (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  else
	  (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  end
          )
	  end)),2) AS BOUNTY_USD_CONVERTED,
	  round((case when jp.currency_code = 'USD' then hm.salary else
      (case when HM.HIRE_DATE > CURRENT_DATE then 
      (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  else
	  (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  end) end),2) as SALARY_USD_CONVERTED2,
	  case when JP.FLAT_FEE_FLAG = 'Y' then 'Fixed' else cast(HM.SALARY_PERCENT_AWARD as varchar(256)) end AS SALARY_PERCENT_AWARD,
	  case when JP.FLAT_FEE_FLAG = 'Y' then 'Fixed' else 'Percentage' end as FEE_TYPE,
	  case when JP.FLAT_FEE_FLAG = 'Y' then JP.BOUNTY else HM.SALARY * (HM.SALARY_PERCENT_AWARD / 100) end as AWARD_AMOUNT,
	  (SELECT SUM(CASE WHEN A.ACCOUNT_TYPE_CODE = 'D' THEN AE.AMOUNT ELSE -AE.AMOUNT END)
       FROM {{ source('bounty_jobs', 'transaction') }} T
                JOIN {{ source('bounty_jobs', 'account_entry') }} AE ON AE.TRANSACTION_ID = T.ID
                JOIN {{ source('bounty_jobs', 'account') }} A ON A.ID = AE.ACCOUNT_ID
           AND A.ACCOUNT_CHART_TYPE_ID IN (1, 2, 4)
                JOIN {{ source('bounty_jobs', 'company') }} C ON C.COMPANY_ID = A.COMPANY_ID
           AND C.COMPANY_TYPE_CODE = 'E'
       WHERE T.SUBMISSION_ID = HM.SUBMISSION_ID
         AND T.TRANSACTION_TYPE_ID IN (6, 12, 24, 25, 26, 27, 29, 30, 31, 32, 33, 36)) AS SPEND_AMOUNT,
	  round((case when JP.FLAT_FEE_FLAG = 'Y' then (BOUNTY * (case when HM.CURRENCY_CODE = 'USD' then 1
	  else (case when HM.HIRE_DATE > CURRENT_DATE then 
      (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  else
	  (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  end
    ) end )) else (HM.SALARY * (case when HM.HIRE_DATE > CURRENT_DATE then 
      (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  else
	  (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  end )) * (HM.SALARY_PERCENT_AWARD / 100) end),2) AS FEE_AMOUNT_USD_CONVERTED,
	  JP.CURRENCY_CODE,
	  case when JP.CURRENCY_CODE = 'USD' then 
      (case when HM.HIRE_DATE > CURRENT_DATE then JP.CREATION_DATE else HM.HIRE_DATE end)
      else
      (case when HM.HIRE_DATE > CURRENT_DATE then 
      (case when HM.CURRENCY_CODE = 'USD' then COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE) else (SELECT CR.EFFECTIVE_DATE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  else
	  (case when HM.CURRENCY_CODE = 'USD' then COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE) else (SELECT CR.EFFECTIVE_DATE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  end ) end as CURRENCY_CONVERSION_DATE,
	  round((case when JP.CURRENCY_CODE = 'USD' then 1
    else (case when HM.HIRE_DATE > CURRENT_DATE then 
      (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  else
	  (case when HM.CURRENCY_CODE = 'USD' then 1 else (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
              WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
              ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
	  end ) end),2) AS CURRENCY_CONVERSION_RATE,
	  CASE
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 6 AND (DATEDIFF(day,ORIG_REC.INITIAL_ACTIVATION_DATE,S.CREATION_DATE) < 15 and
                                                           ORIG_REC.INITIAL_ACTIVATION_DATE < '2008-02-01 00:00:00' AND
                                                           S.CREATION_DATE > '2007-02-01 00:00:00')
            THEN '100% Quick Draw'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 6 AND (DATEDIFF(day,ORIG_REC.INITIAL_ACTIVATION_DATE,S.CREATION_DATE) < 15 and
                                                           ORIG_REC.INITIAL_ACTIVATION_DATE > '2008-02-01 00:00:00' AND
                                                           ORIG_REC.INITIAL_ACTIVATION_DATE < '2010-07-24 00:00:00')
            THEN '97% Quick Draw'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 6 AND CT.CONTRACT_ID IS NOT NULL AND
             DATEDIFF(day,ORIG_REC.INITIAL_ACTIVATION_DATE,S.CREATION_DATE) > 14 AND CT.START_DATE < S.CREATION_DATE
            THEN 'Special Contract'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 24 THEN '97% Quick Draw'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 25 THEN 'Special Contract'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 26 THEN 'Employer Gets Discount'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 27 THEN 'Multi-fill Deal'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 29 THEN 'Payout Override'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 36 THEN 'Retained'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID IS NULL THEN 'Not Processed Yet'
        WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 40 THEN 'Split'
        ELSE
            CASE WHEN HIRE_TRANSACTION.TRANSACTION_TYPE_ID = 6 THEN 'Standard Terms'
                 ELSE 'Unknown'
                END
        END AS TYPE_OF_PLACEMENT,
	  cast(COALESCE(BJ_AMOUNT.AMOUNT_SUM,
          ((case when JP.FLAT_FEE_FLAG = 'Y' then JP.BOUNTY else HM.SALARY * (HM.SALARY_PERCENT_AWARD / 100) end) -
          (case when JP.RETAINED_FLAG='Y' then (
              SELECT -- subtract out what was already paid
                     SUM(AMOUNT)
              FROM {{ source('bounty_jobs', 'account_entry') }} AE
                       JOIN {{ source('bounty_jobs', 'transaction') }} T ON AE.TRANSACTION_ID = T.ID AND TRANSACTION_TYPE_ID=35
                       JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
                       JOIN {{ source('bounty_jobs', 'account_chart_type') }} ACT ON A.ACCOUNT_CHART_TYPE_ID = ACT.ID
              WHERE T.JOB_POSTING_ID=JP.JOB_POSTING_ID
                AND (case when HM_COMP.RETAINED_GROSS_UP_FLAG='Y' then
                       A.COMPANY_ID=ORIG_REC.COMPANY_ID else
                       A.COMPANY_ID=HM_COMP.COMPANY_ID AND ACT.LOOKUP_ID != 4200 end)
                       ) else 0 end
                       ))
                       * CASE
                            WHEN JP.WORKFLOW_HANDLER_NAME IN ('SplitPosting') THEN 0.10
                            ELSE
                                (case when CT.CONTRACT_ID IS NOT NULL AND CTC.ID IS NOT NULL AND CT.START_DATE < S.CREATION_DATE then CTC.PERCENTAGE_AMOUNT else TC.PERCENTAGE_AMOUNT end)
                      END ) AS DECIMAL(20,2)) AS BJ_NET,
    round(COALESCE(BJ_AMOUNT.AMOUNT_SUM,
         ((CAST((case when JP.FLAT_FEE_FLAG = 'Y' then JP.BOUNTY 
                     else HM.SALARY * (HM.SALARY_PERCENT_AWARD / 100) 
                end) AS NUMERIC(18,2)) -
           CAST((case when JP.RETAINED_FLAG='Y' then (
               SELECT COALESCE(SUM(AMOUNT), 0)
               FROM {{ source('bounty_jobs', 'account_entry') }} AE
                        JOIN {{ source('bounty_jobs', 'transaction') }} T ON AE.TRANSACTION_ID = T.ID AND TRANSACTION_TYPE_ID=35
                        JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
                        JOIN {{ source('bounty_jobs', 'account_chart_type') }} ACT ON A.ACCOUNT_CHART_TYPE_ID = ACT.ID
               WHERE T.JOB_POSTING_ID=JP.JOB_POSTING_ID
                 AND (case when HM_COMP.RETAINED_GROSS_UP_FLAG='Y' then
                        A.COMPANY_ID=ORIG_REC.COMPANY_ID else
                        A.COMPANY_ID=HM_COMP.COMPANY_ID AND ACT.LOOKUP_ID != 4200 end)
           ) else 0 end) AS NUMERIC(18,2)))
         * CAST(CASE
                WHEN JP.WORKFLOW_HANDLER_NAME IN ('SplitPosting') THEN 0.10
                ELSE
                    (case when CT.CONTRACT_ID IS NOT NULL AND CTC.ID IS NOT NULL AND CT.START_DATE < S.CREATION_DATE 
                          then CTC.PERCENTAGE_AMOUNT 
                          else TC.PERCENTAGE_AMOUNT 
                     end)
              END AS NUMERIC(5,4)) 
         * CAST(
              case when HM.CURRENCY_CODE = 'USD' then 1 else
              (case when HM.HIRE_DATE > CURRENT_DATE then 
                (case when HM.CURRENCY_CODE = 'USD' then 1 else 
                      (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
                       WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                         AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                         AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
                       ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
              else
                (case when HM.CURRENCY_CODE = 'USD' then 1 else 
                      (SELECT CR.VALUE FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
                       WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                         AND CR.TARGET_CURRENCY_CODE = UPPER(TRIM('USD'))
                         AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE,HM.HIRE_DATE)
                       ORDER BY CR.EFFECTIVE_DATE DESC LIMIT 1) end)
              end ) end AS NUMERIC(18,6))
        )), 2) AS BJ_NET_USD_CONVERTED,
	  cast(COALESCE(HM_AMOUNT.AMOUNT_SUM,
          ((case when JP.FLAT_FEE_FLAG = 'Y' then JP.BOUNTY else HM.SALARY * (HM.SALARY_PERCENT_AWARD / 100) end) -
          (case when JP.RETAINED_FLAG='Y' then (
              SELECT -- subtract out what was already paid
                     SUM(AMOUNT)
              FROM {{ source('bounty_jobs', 'account_entry') }} AE
                       JOIN {{ source('bounty_jobs', 'transaction') }} T ON AE.TRANSACTION_ID = T.ID AND TRANSACTION_TYPE_ID=35
                       JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
                       JOIN {{ source('bounty_jobs', 'account_chart_type') }} ACT ON A.ACCOUNT_CHART_TYPE_ID = ACT.ID
              WHERE T.JOB_POSTING_ID=JP.JOB_POSTING_ID
                AND (case when HM_COMP.RETAINED_GROSS_UP_FLAG='Y' then
                       A.COMPANY_ID=ORIG_REC.COMPANY_ID else
                       A.COMPANY_ID=HM_COMP.COMPANY_ID AND ACT.LOOKUP_ID != 4200 end)
                       ) else 0 end
                       ))
                       * CASE
                            WHEN JP.WORKFLOW_HANDLER_NAME IN ('SplitPosting') THEN 1.05
                            WHEN JP.RETAINED_FLAG='Y' AND HM_COMP.RETAINED_GROSS_UP_FLAG='Y' THEN
                                    1.00 + (case when CT.CONTRACT_ID IS NOT NULL AND CTC.ID IS NOT NULL AND CT.START_DATE < S.CREATION_DATE then CTC.PERCENTAGE_AMOUNT else TC.PERCENTAGE_AMOUNT end)
                            ELSE
                                1.00
                      END ) AS DECIMAL(20,2)) AS EMPLOYER_NET,
   ROUND((
  COALESCE(
    HM_AMOUNT.AMOUNT_SUM,
    (
      CAST(
        (
          CASE 
            WHEN JP.FLAT_FEE_FLAG = 'Y' THEN JP.BOUNTY
            ELSE HM.SALARY * (HM.SALARY_PERCENT_AWARD / 100)
          END
        ) AS NUMERIC(18, 2)
      )
      -
      CAST(
        (
          CASE 
            WHEN JP.RETAINED_FLAG = 'Y' THEN (
              SELECT COALESCE(SUM(AMOUNT), 0)
              FROM {{ source('bounty_jobs', 'account_entry') }} AE
              JOIN {{ source('bounty_jobs', 'transaction') }} T ON AE.TRANSACTION_ID = T.ID AND TRANSACTION_TYPE_ID = 35
              JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
              JOIN {{ source('bounty_jobs', 'account_chart_type') }} ACT ON A.ACCOUNT_CHART_TYPE_ID = ACT.ID
              WHERE T.JOB_POSTING_ID = JP.JOB_POSTING_ID
                AND (
                  CASE 
                    WHEN HM_COMP.RETAINED_GROSS_UP_FLAG = 'Y' THEN A.COMPANY_ID = ORIG_REC.COMPANY_ID 
                    ELSE A.COMPANY_ID = HM_COMP.COMPANY_ID AND ACT.LOOKUP_ID != 4200 
                  END
                )
            )
            ELSE 0
          END
        ) AS NUMERIC(18, 2)
      )
    )
    *
    CAST(
      (
        CASE
          WHEN JP.WORKFLOW_HANDLER_NAME IN ('SplitPosting') THEN 1.05
          WHEN JP.RETAINED_FLAG = 'Y' AND HM_COMP.RETAINED_GROSS_UP_FLAG = 'Y' THEN 
            1.00 + (
              CASE 
                WHEN CT.CONTRACT_ID IS NOT NULL AND CTC.ID IS NOT NULL AND CT.START_DATE < S.CREATION_DATE THEN CTC.PERCENTAGE_AMOUNT 
                ELSE TC.PERCENTAGE_AMOUNT 
              END
            )
          ELSE 1.00
        END
      ) AS NUMERIC(18, 4)
    )
    *
    CAST(
      (
        CASE 
          WHEN HM.CURRENCY_CODE = 'USD' THEN 1 
          ELSE 
            (
              CASE 
                WHEN HM.HIRE_DATE > CURRENT_DATE THEN 
                  (
                    CASE 
                      WHEN HM.CURRENCY_CODE = 'USD' THEN 1 
                      ELSE (
                        SELECT CR.VALUE 
                        FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
                        WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                          AND CR.TARGET_CURRENCY_CODE = 'USD'
                          AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
                        ORDER BY CR.EFFECTIVE_DATE DESC 
                        LIMIT 1
                      )
                    END
                  )
                ELSE 
                  (
                    CASE 
                      WHEN HM.CURRENCY_CODE = 'USD' THEN 1 
                      ELSE (
                        SELECT CR.VALUE 
                        FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
                        WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                          AND CR.TARGET_CURRENCY_CODE = 'USD'
                          AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE, HM.HIRE_DATE)
                        ORDER BY CR.EFFECTIVE_DATE DESC 
                        LIMIT 1
                      )
                    END
                  )
              END
            )
        END
      ) AS NUMERIC(18, 6)
    )
  )
), 2) AS EMPLOYER_NET_USD_CONVERTED,
	  cast(COALESCE(HH_AMOUNT.AMOUNT_SUM,
          ((case when JP.FLAT_FEE_FLAG = 'Y' then JP.BOUNTY else HM.SALARY * (HM.SALARY_PERCENT_AWARD / 100) end) -
          (case when JP.RETAINED_FLAG='Y' then (
              SELECT -- subtract out what was already paid
                     SUM(AMOUNT)
              FROM {{ source('bounty_jobs', 'account_entry') }} AE
                       JOIN {{ source('bounty_jobs', 'transaction') }} T ON AE.TRANSACTION_ID = T.ID AND TRANSACTION_TYPE_ID=35
                       JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
                       JOIN {{ source('bounty_jobs', 'account_chart_type') }} ACT ON A.ACCOUNT_CHART_TYPE_ID = ACT.ID
              WHERE T.JOB_POSTING_ID=JP.JOB_POSTING_ID
                AND (case when HM_COMP.RETAINED_GROSS_UP_FLAG='Y' then
                       A.COMPANY_ID=ORIG_REC.COMPANY_ID else
                       A.COMPANY_ID=HM_COMP.COMPANY_ID AND ACT.LOOKUP_ID IN (1100, 2200, 2300) end)
                       ) else 0 end
                       ))
                       * CASE
                            WHEN JP.WORKFLOW_HANDLER_NAME IN ('SplitPosting') THEN 0.95
                            WHEN JP.RETAINED_FLAG='Y' AND HM_COMP.RETAINED_GROSS_UP_FLAG='Y' THEN 1.00 
                            ELSE
                                (1.00 - (case when CT.CONTRACT_ID IS NOT NULL AND CTC.ID IS NOT NULL AND CT.START_DATE < S.CREATION_DATE then CTC.PERCENTAGE_AMOUNT else TC.PERCENTAGE_AMOUNT end))
                      END ) AS DECIMAL(20,2)) AS RECRUITER_NET,
    ROUND((
  COALESCE(
    HH_AMOUNT.AMOUNT_SUM,
    (
      CAST(
        (
          CASE 
            WHEN JP.FLAT_FEE_FLAG = 'Y' THEN JP.BOUNTY
            ELSE HM.SALARY * (HM.SALARY_PERCENT_AWARD / 100)
          END
        ) AS NUMERIC(18, 2)
      )
      -
      CAST(
        (
          CASE 
            WHEN JP.RETAINED_FLAG = 'Y' THEN (
              SELECT COALESCE(SUM(AMOUNT), 0)
              FROM {{ source('bounty_jobs', 'account_entry') }} AE
              JOIN {{ source('bounty_jobs', 'transaction') }} T 
                ON AE.TRANSACTION_ID = T.ID AND TRANSACTION_TYPE_ID = 35
              JOIN {{ source('bounty_jobs', 'account') }} A 
                ON AE.ACCOUNT_ID = A.ID
              JOIN {{ source('bounty_jobs', 'account_chart_type') }} ACT 
                ON A.ACCOUNT_CHART_TYPE_ID = ACT.ID
              WHERE T.JOB_POSTING_ID = JP.JOB_POSTING_ID
                AND (
                  CASE 
                    WHEN HM_COMP.RETAINED_GROSS_UP_FLAG = 'Y' THEN A.COMPANY_ID = ORIG_REC.COMPANY_ID
                    ELSE A.COMPANY_ID = HM_COMP.COMPANY_ID AND ACT.LOOKUP_ID IN (1100, 2200, 2300)
                  END
                )
            )
            ELSE 0
          END
        ) AS NUMERIC(18, 2)
      )
    )
    *
    CAST(
      (
        CASE
          WHEN JP.WORKFLOW_HANDLER_NAME IN ('SplitPosting') THEN 0.95
          WHEN JP.RETAINED_FLAG = 'Y' AND HM_COMP.RETAINED_GROSS_UP_FLAG = 'Y' THEN 1.00
          ELSE 1.00 - (
            CASE 
              WHEN CT.CONTRACT_ID IS NOT NULL AND CTC.ID IS NOT NULL AND CT.START_DATE < S.CREATION_DATE 
              THEN CTC.PERCENTAGE_AMOUNT 
              ELSE TC.PERCENTAGE_AMOUNT 
            END
          )
        END
      ) AS NUMERIC(18, 4)
    )
    *
    CAST(
      (
        CASE 
          WHEN HM.CURRENCY_CODE = 'USD' THEN 1
          ELSE (
            CASE 
              WHEN HM.HIRE_DATE > CURRENT_DATE THEN (
                CASE 
                  WHEN HM.CURRENCY_CODE = 'USD' THEN 1
                  ELSE (
                    SELECT CR.VALUE
                    FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
                    WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                      AND CR.TARGET_CURRENCY_CODE = 'USD'
                      AND CR.EFFECTIVE_DATE <= COALESCE(JP.POSTING_EFFECTIVE_DATE, JP.CREATION_DATE)
                    ORDER BY CR.EFFECTIVE_DATE DESC
                    LIMIT 1
                  )
                END
              )
              ELSE (
                CASE 
                  WHEN HM.CURRENCY_CODE = 'USD' THEN 1
                  ELSE (
                    SELECT CR.VALUE
                    FROM {{ source('bounty_jobs', 'conversion_rate') }} CR
                    WHERE CR.SOURCE_CURRENCY_CODE = UPPER(TRIM(HM.CURRENCY_CODE))
                      AND CR.TARGET_CURRENCY_CODE = 'USD'
                      AND CR.EFFECTIVE_DATE <= GREATEST(DATE_TRUNC('month', HMM.CREATION_DATE)::DATE, HM.HIRE_DATE)
                    ORDER BY CR.EFFECTIVE_DATE DESC
                    LIMIT 1
                  )
                END
              )
            END
          )
        END
      ) AS NUMERIC(18, 6)
    )
  )
), 2) AS RECRUITER_NET_USD_CONVERTED,
	  CIR.WORKED_IN_CANADA_RESPONSE AS WORKED_IN_CANADA_RESPONSE,
    CTR.WORKED_IN_QUEBEC_RESPONSE AS WORKED_IN_QUEBEC_RESPONSE,
    CTR.CANADIAN_TAX_ID_NUMBER AS CANADIAN_TAX_ID_NUMBER,
    CTR.AGENCY_BOUNTY_PORTION AS AGENCY_BOUNTY_PORTION,
    CTR.PST_SELECTED AS PST_SELECTED,
    CTR.PST_APPLICABLE_TAX_AMOUNT AS PST_APPLICABLE_TAX_AMOUNT,
    CTR.PST_PERCENT,
    CTR.GST_SELECTED AS GST_SELECTED,
    CTR.GST_APPLICABLE_TAX_AMOUNT AS GST_APPLICABLE_TAX_AMOUNT,
    CTR.GST_PERCENT,
    CTR.HST_SELECTED AS HST_SELECTED,
    CTR.HST_APPLICABLE_TAX_AMOUNT AS HST_APPLICABLE_TAX_AMOUNT,
    CTR.HST_PERCENT,
    CTR.NOTES,
    (
      SELECT
        LISTAGG(
          AQ.TEXT || ': ' || SAQA.ANSWER,
          '; '
        ) WITHIN GROUP (
          ORDER BY SAQA.AWARD_QUESTION_ID
        )
      FROM {{ source('bounty_jobs', 'submission_award_question_answer') }} SAQA
      JOIN {{ source('bounty_jobs', 'company_award_question') }} CAQ
        ON CAQ.AWARD_QUESTION_ID = SAQA.AWARD_QUESTION_ID
      JOIN {{ source('bounty_jobs', 'award_question') }} AQ
        ON AQ.AWARD_QUESTION_ID = SAQA.AWARD_QUESTION_ID
      WHERE SAQA.SUBMISSION_ID = S.SUBMISSION_ID
        AND CAQ.REQUIRED = 1
        AND CAQ.COMPANY_ID = HM_COMP.COMPANY_ID
    ) AS REQUIRED_AWARD_FIELDS,
    (
      SELECT
        LISTAGG(
          AQ.TEXT || ': ' || SAQA.ANSWER,
          '; '
        ) WITHIN GROUP (
          ORDER BY SAQA.AWARD_QUESTION_ID
        )
      FROM {{ source('bounty_jobs', 'submission_award_question_answer') }} SAQA
      JOIN {{ source('bounty_jobs', 'company_award_question') }} CAQ
        ON CAQ.AWARD_QUESTION_ID = SAQA.AWARD_QUESTION_ID
      JOIN {{ source('bounty_jobs', 'award_question') }} AQ
        ON AQ.AWARD_QUESTION_ID = SAQA.AWARD_QUESTION_ID
      WHERE SAQA.SUBMISSION_ID = S.SUBMISSION_ID
        AND CAQ.REQUIRED = 0
        AND CAQ.COMPANY_ID = HM_COMP.COMPANY_ID
    ) AS OPTIONAL_AWARD_FIELDS,
	  (SELECT SAQA.ANSWER
       FROM {{ source('bounty_jobs', 'submission_award_question_answer') }} SAQA
       WHERE SAQA.SUBMISSION_ID = S.SUBMISSION_ID
         AND SAQA.AWARD_QUESTION_ID = 2) AS AWARD_QUESTION_HIRING_MANAGER,
      (SELECT SAQA.ANSWER
       FROM {{ source('bounty_jobs', 'submission_award_question_answer') }} SAQA
       WHERE SAQA.SUBMISSION_ID = S.SUBMISSION_ID
         AND SAQA.AWARD_QUESTION_ID = 24) AS AWARD_QUESTION_INTERNAL_JOB_ID,
      (SELECT SAQA.ANSWER
       FROM {{ source('bounty_jobs', 'submission_award_question_answer') }} SAQA
       WHERE SAQA.SUBMISSION_ID = S.SUBMISSION_ID
         AND SAQA.AWARD_QUESTION_ID = 25) AS AWARD_QUESTION_BJ_PURCHASE_ORDER
FROM {{ source('bounty_jobs', 'hire_message') }} HM
         JOIN {{ source('bounty_jobs', 'message') }} HMM ON HM.MESSAGE_ID = HMM.MESSAGE_ID
         JOIN {{ source('bounty_jobs', 'submission') }} S ON HM.SUBMISSION_ID = S.SUBMISSION_ID
         JOIN {{ source('bounty_jobs', 'job_posting') }} JP ON S.JOB_POSTING_ID = JP.JOB_POSTING_ID AND JP.WORKFLOW_HANDLER_NAME NOT IN ('ContractPosting','AtsManualContractPosting')
         JOIN {{ source('bounty_jobs', 'company') }} HM_COMP ON JP.COMPANY_ID = HM_COMP.COMPANY_ID
         JOIN {{ source('bounty_jobs', 'candidate') }} C ON S.CANDIDATE_ID = C.CANDIDATE_ID
         JOIN {{ source('bounty_jobs', 'recruiter') }} REC ON C.RECRUITER_PERSON_ID = REC.PERSON_ID
         JOIN {{ source('bounty_jobs', 'person') }} ORIG_REC ON REC.PERSON_ID = ORIG_REC.PERSON_ID
         JOIN {{ source('bounty_jobs', 'transaction_cost') }} TC ON (case when JP.RETAINED_FLAG='Y' then TC.LOOKUP_KEY='BOUNTYFEEHRET' AND TC.TRANSACTION_TYPE_ID=36 else TC.LOOKUP_KEY='BOUNTYFEEH' AND TC.TRANSACTION_TYPE_ID=6 end)
         LEFT JOIN (
    SELECT
        MAX(T.ID) AS ID,
        T.TRANSACTION_TYPE_ID,
        T.SUBMISSION_ID AS SUBMISSION_ID
    FROM {{ source('bounty_jobs', 'transaction') }} T
    WHERE T.TRANSACTION_TYPE_ID IN (6, 24, 25, 26, 27, 29, 36, 40)
    GROUP BY T.SUBMISSION_ID,t.transaction_type_id
) HIRE_TRANSACTION ON HIRE_TRANSACTION.SUBMISSION_ID = S.SUBMISSION_ID
         LEFT JOIN {{ source('bounty_jobs', 'contract') }} CT ON
        (((JP.COMPANY_ID = CT.SOURCE_PARTY_ID OR JP.HIRING_MANAGER_PERSON_ID = CT.SOURCE_PARTY_ID)
            AND (ORIG_REC.COMPANY_ID = CT.TARGET_PARTY_ID OR ORIG_REC.PERSON_ID = CT.TARGET_PARTY_ID))
            OR ((JP.COMPANY_ID = CT.TARGET_PARTY_ID OR JP.HIRING_MANAGER_PERSON_ID = CT.TARGET_PARTY_ID)
                AND (ORIG_REC.COMPANY_ID = CT.SOURCE_PARTY_ID OR ORIG_REC.PERSON_ID = CT.SOURCE_PARTY_ID)))
        AND S.CREATION_DATE >= CT.START_DATE
        AND S.CREATION_DATE <= CT.END_DATE
         LEFT JOIN {{ source('bounty_jobs', 'company_transaction_cost') }} CTC ON CTC.TRANSACTION_COST_ID = TC.ID AND CT.CONTRACT_ID = CTC.CONTRACT_ID
         LEFT JOIN {{ source('bounty_jobs', 'message') }} HRM ON HRM.PARENT_MESSAGE_ID=HMM.MESSAGE_ID
         LEFT JOIN {{ source('bounty_jobs', 'hire_response') }} HR ON HRM.MESSAGE_ID=HR.MESSAGE_ID
         LEFT JOIN {{ source('bounty_jobs', 'hire_response_type') }} HRT ON HR.HIRE_RESPONSE_TYPE_CODE = HRT.CODE
         LEFT JOIN (
    SELECT AE.TRANSACTION_ID,
           SUM(case when A.ACCOUNT_TYPE_CODE='C' then AE.AMOUNT else -AE.AMOUNT end) AS AMOUNT_SUM
    FROM {{ source('bounty_jobs', 'account_entry') }} AE
             JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
             JOIN {{ source('bounty_jobs', 'account_chart_type') }} T ON A.ACCOUNT_CHART_TYPE_ID = T.ID
        AND T.LOOKUP_ID = 4200 -- Bounty revenue
             JOIN {{ source('bounty_jobs', 'company') }} C ON A.COMPANY_ID = C.COMPANY_ID AND C.COMPANY_TYPE_CODE='E'
    GROUP BY TRANSACTION_ID
) BJ_AMOUNT ON BJ_AMOUNT.TRANSACTION_ID=HIRE_TRANSACTION.ID AND HRT.CODE='A'
         LEFT JOIN (
    SELECT AE.TRANSACTION_ID,
           SUM(case when A.ACCOUNT_TYPE_CODE='D' then AE.AMOUNT else -AE.AMOUNT end) AS AMOUNT_SUM
    FROM {{ source('bounty_jobs', 'account_entry') }} AE
             JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
             JOIN {{ source('bounty_jobs', 'account_chart_type') }} T ON A.ACCOUNT_CHART_TYPE_ID = T.ID
        AND T.LOOKUP_ID != 4200 -- Not Bounty Revenue
             JOIN {{ source('bounty_jobs', 'company') }} C ON A.COMPANY_ID = C.COMPANY_ID AND C.COMPANY_TYPE_CODE='E'
    GROUP BY TRANSACTION_ID
) HM_AMOUNT ON HM_AMOUNT.TRANSACTION_ID=HIRE_TRANSACTION.ID AND HRT.CODE='A'
         LEFT JOIN (
    SELECT AE.TRANSACTION_ID,
           SUM(case when A.ACCOUNT_TYPE_CODE='C' then AE.AMOUNT else -AE.AMOUNT end) AS AMOUNT_SUM
    FROM {{ source('bounty_jobs', 'account_entry') }} AE
             JOIN {{ source('bounty_jobs', 'account') }} A ON AE.ACCOUNT_ID = A.ID
             JOIN {{ source('bounty_jobs', 'company') }} C ON A.COMPANY_ID = C.COMPANY_ID AND C.COMPANY_TYPE_CODE='R'
    GROUP BY TRANSACTION_ID
) HH_AMOUNT ON HH_AMOUNT.TRANSACTION_ID=HIRE_TRANSACTION.ID AND HRT.CODE='A'
         LEFT JOIN {{ source('bounty_jobs', 'canadian_invoicing_response') }} CIR ON S.SUBMISSION_ID = CIR.SUBMISSION_ID
         LEFT JOIN {{ source('bounty_jobs', 'canadian_tax_response') }} CTR ON CTR.CANADIAN_INVOICING_RESPONSE_ID = CIR.ID