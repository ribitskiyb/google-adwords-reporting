package etl.cloud.google.adwords;

import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration;
import com.google.api.ads.adwords.lib.jaxb.v201708.DownloadFormat;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponse;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponseException;
import com.google.api.ads.adwords.lib.utils.ReportException;
import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.ads.common.lib.exception.OAuthException;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;

import java.io.IOException;


public class ReportDownloader {

    public ReportDownloader(String clientId, String clientSecret, String developerToken, String refreshToken)
            throws ValidationException, OAuthException {
        // Generate a refreshable OAuth2 credential.
        Credential oAuth2Credential = new OfflineCredentials.Builder()
                .forApi(Api.ADWORDS)
                .withRefreshToken(refreshToken)
                .withClientSecrets(clientId, clientSecret)
                .build()
                .generateCredential();

        // Construct an AdWordsSession.
        this.session = new AdWordsSession.Builder()
                .withDeveloperToken(developerToken)
                .withOAuth2Credential(oAuth2Credential)
                .build();
    }

    public void download(String clientCustomerId, String reportType, String reportFields,
                         String reportStartDate, String reportEndDate, String outputFile)
            throws ReportDownloadResponseException, ReportException, IOException {
        // Create query.
        String query = String.format(
                "SELECT %s FROM %s DURING %s,%s"
                ,reportFields
                ,reportType
                ,reportStartDate
                ,reportEndDate
        );

        // Optional: Set the reporting configuration of the session to suppress header, column name, or
        // summary rows in the report output. You can also configure this via your ads.properties
        // configuration file. See AdWordsSession.Builder.from(Configuration) for details.
        // In addition, you can set whether you want to explicitly include or exclude zero impression
        // rows.
        ReportingConfiguration reportingConfiguration =
                new ReportingConfiguration.Builder()
                        .skipReportHeader(true)
                        .skipColumnHeader(false)
                        .skipReportSummary(true)
                        // Set to false to exclude rows with zero impressions.
                        //.includeZeroImpressions(true)
                        .build();
        session.setReportingConfiguration(reportingConfiguration);
        session.setClientCustomerId(clientCustomerId);

        // Set the property api.adwords.reportDownloadTimeout or call
        // ReportDownloader.setReportDownloadTimeout to set a timeout (in milliseconds)
        // for CONNECT and READ in report downloads.
        ReportDownloadResponse response =
                new com.google.api.ads.adwords.lib.utils.v201708.ReportDownloader(session).downloadReport(
                        query,
                        DownloadFormat.CSV
                );
        response.saveToFile(outputFile);
    }

    private AdWordsSession session;
}
