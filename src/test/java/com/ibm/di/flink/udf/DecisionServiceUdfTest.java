/*
 *
 *   Copyright IBM Corp. 2026
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package com.ibm.di.flink.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Loan Approval decision service archive that Maven downloads to
 * {@code target/decision-service/} (see {@code download-decision-service} in the POM).
 * The tests fail, rather than skip, if the archive is missing.
 */
class DecisionServiceUdfTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    // Same input as the Flink SQL example, for a loan of {amount} and a borrower with {creditScore}.
    private static final String LOAN_APPLICATION = """
        {"loan":{"amount":%d,"numberOfMonthlyPayments":72,"startDate":"2005-06-01T00:00:00Z","loanToValue":0.7},
         "borrower":{"firstName":"John","lastName":"Doe","yearlyIncome":100000,
                     "zipCode":"91320","creditScore":%d}}
        """;

    private DecisionServiceUdf udf;
    private String archiveUri;

    @BeforeEach
    void setUp() {
        File archive = new File("target/decision-service/loanApproval.jar");
        assertTrue(archive.isFile(), "Decision service archive not downloaded: " + archive.getAbsolutePath()
            + ". Run 'mvn test' (the download is part of the build).");
        archiveUri = archive.toURI().toString();
        udf = new DecisionServiceUdf();
        udf.open(null);
    }

    @AfterEach
    void tearDown() {
        udf.close();
    }

    @Test
    void approvesALoan() throws Exception {
        JsonNode decision = decide("approval", LOAN_APPLICATION.formatted(185000, 500));

        assertNull(decision.get("error"), decision.toString());
        assertTrue(decision.at("/approval/approved").asBoolean(), decision.toString());
        assertEquals("Congratulations! Your loan has been approved", decision.at("/approval/message").asText());
    }

    @Test
    void refusesALoan() throws Exception {
        JsonNode decision = decide("approval", LOAN_APPLICATION.formatted(200000, 700));

        assertNull(decision.get("error"), decision.toString());
        assertFalse(decision.at("/approval/approved").asBoolean(), decision.toString());
        assertTrue(decision.at("/approval/message").asText().startsWith("Too big Debt/Income ratio"));
    }

    @Test
    void reusesTheLoadedArchiveAcrossCalls() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertTrue(decide("approval", LOAN_APPLICATION.formatted(185000, 500))
                .at("/approval/approved").asBoolean());
        }
    }

    @Test
    void reportsAnUnknownOperation() throws Exception {
        assertError(decide("noSuchOperation", LOAN_APPLICATION.formatted(185000, 500)), "noSuchOperation");
    }

    @Test
    void reportsAnArchiveThatDoesNotExist() throws Exception {
        assertError(JSON.readTree(udf.eval("approval", "file:///no/such/archive.jar", "{}")), "");
    }

    @Test
    void reportsInvalidInputJson() throws Exception {
        assertError(decide("approval", "not json {"), "");
    }

    @Test
    void reportsNullArguments() throws Exception {
        assertError(JSON.readTree(udf.eval(null, archiveUri, "{}")), "null");
        assertError(JSON.readTree(udf.eval("approval", null, "{}")), "null");
        assertError(JSON.readTree(udf.eval("approval", archiveUri, null)), "null");
    }

    private JsonNode decide(String operation, String inputJson) throws Exception {
        return JSON.readTree(udf.eval(operation, archiveUri, inputJson));
    }

    private static void assertError(JsonNode result, String expectedText) {
        JsonNode error = result.get("error");
        assertNotNull(error, "Expected an error result, got: " + result);
        assertTrue(error.asText().contains(expectedText), "Unexpected error message: " + error.asText());
    }
}
