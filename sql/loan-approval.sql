--
--   Copyright IBM Corp. 2026
--
--   Licensed under the Apache License, Version 2.0 (the "License");
--   you may not use this file except in compliance with the License.
--   You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- Calls the "approval" operation of the Loan Approval decision service from Flink SQL.
--
--   docker compose exec -T jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/sql/loan-approval.sql

-- Run as a batch query so that the script ends once the result is printed.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.display.max-column-width' = '150';

-- 1. Register the UDF. The JAR contains DecisionServiceUdf, not the decision service.
CREATE TEMPORARY FUNCTION execute_decision
  AS 'com.ibm.di.flink.udf.DecisionServiceUdf'
  LANGUAGE JAVA
  USING JAR 'file:///opt/flink/udfs/flink-decision-udf.jar';

-- 2. The input: two loan applications for the same borrower.
CREATE TEMPORARY VIEW loan_applications AS
SELECT * FROM (VALUES
  ('small loan, low score', 185000, 500),
  ('large loan, high score', 200000, 700)
) AS t(application, amount, creditScore);

-- 3. Call the decision service for each row.
--    execute_decision(operation, decision service archive, input as JSON) returns the decision as JSON.
CREATE TEMPORARY VIEW decisions AS
SELECT
  application,
  execute_decision(
    'approval',                                            -- operation to run
    'file:///opt/flink/decision-service/loanApproval.jar', -- the decision service archive
    JSON_OBJECT(                                           -- the decision input
      'loan' VALUE JSON_OBJECT(
        'amount'                  VALUE amount,
        'numberOfMonthlyPayments' VALUE 72,
        'startDate'               VALUE '2005-06-01T00:00:00Z',
        'loanToValue'             VALUE 0.7
      ),
      'borrower' VALUE JSON_OBJECT(
        'firstName'    VALUE 'John',
        'lastName'     VALUE 'Doe',
        'yearlyIncome' VALUE 100000,
        'zipCode'      VALUE '91320',
        'creditScore'  VALUE creditScore
      )
    )
  ) AS decision_json
FROM loan_applications;

-- 4. Show the decision. JSON_VALUE reads fields of the output.
SELECT
  application,
  JSON_VALUE(decision_json, '$.approval.approved') AS approved,
  JSON_VALUE(decision_json, '$.approval.message')  AS message,
  decision_json
FROM decisions;
