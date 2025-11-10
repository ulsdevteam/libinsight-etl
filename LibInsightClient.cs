using Flurl.Http;
using Newtonsoft.Json.Linq;

/// <summary>
/// Client class for getting dataset records via the LibInsight API.
/// </summary>
class LibInsightClient
{
    public LibInsightClient()
    {
        Client = new FlurlClient("https://pitt.libinsight.com/v1.0");
    }

    IFlurlClient Client { get; }

    /// <summary>
    /// Call the LibInsight OAuth endpoint to get an access token and store it.
    /// </summary>
    /// <param name="clientId">LibInsight Client Id</param>
    /// <param name="clientSecret">LibInsight Client Secret</param>
    public async Task Authorize(string clientId, string clientSecret)
    {
        var tokenResponse = await Client
            .Request("oauth/token")
            .PostUrlEncodedAsync(new
            {
                client_id = clientId,
                client_secret = clientSecret,
                grant_type = "client_credentials"
            });
        var json = await tokenResponse.GetJsonAsync<JObject>();
        Client.WithHeader("Authorization", "Bearer " + json["access_token"]);
    }

    /// <summary>
    /// Call a LibInsight API and get dataset records.
    /// </summary>
    /// <remarks>
    /// The API endpoint first needs to be set up on the LibInsight "Widgets and APIs" page, in the "Admin" section.
    /// </remarks>
    /// <param name="datasetId">Id of the dataset to retrieve.</param>
    /// <param name="requestId">Id of the API on the LibInsight "Widgets and APIs" page.</param>
    /// <param name="fromDate">First date to pull records from. Inclusive.</param>
    /// <param name="toDate">Last date to pull records from. Inclusive.</param>
    /// <returns>List of records as JSON objects.</returns>
    /// <exception cref="Exception">Thrown if the API call returns an error response.</exception>
    public async Task<List<JObject>> GetCustomDatasetRecords(int datasetId, int requestId, DateTime fromDate, DateTime toDate)
    {
        var records = new List<JObject>();
        for (var page = 1; ; ++page)
        {
            var response = await Client.Request("custom-dataset", datasetId, "data-grid").SetQueryParams(new
            {
                request_id = requestId,
                from = fromDate.ToString("yyyy-MM-dd"),
                to = toDate.ToString("yyyy-MM-dd"),
                entered_by = "all",
                sort = "asc",
                page
            }).GetJsonAsync<JObject>();
            if ((string)response["type"] == "error")
            {
                throw new Exception(response.ToString());
            }
            var payload = response["payload"];
            records.AddRange(payload["records"].Values<JObject>());
            if ((int?)payload["displayed_page"] == (int?)payload["total_pages"])
            {
                return records;
            }
        }
    }

    public async Task<JObject> GetGateCountData(int datasetId, DateTime fromDate, DateTime toDate, string aggregate)
    {
        var response = await Client.Request("gate-count", datasetId, "overview").SetQueryParams(new
        {
            from = fromDate.ToString("yyyy-MM-dd"),
            to = toDate.ToString("yyyy-MM-dd"),
            aggregate
        }
        ).GetJsonAsync<JObject>();
        if ((string)response["type"] == "error")
        {
            throw new Exception(response.ToString());
        }
        return response["payload"] as JObject;
    }
}