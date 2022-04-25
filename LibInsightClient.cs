using Flurl;
using Flurl.Http;
using Newtonsoft.Json.Linq;

class LibInsightClient
{
    IFlurlClient Client { get; }

    public LibInsightClient()
    {
        Client = new FlurlClient("https://pitt.libinsight.com/v1.0");
    }

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

    public async Task<List<JObject>> GetRecords(int datasetId, int requestId, DateTime fromDate, DateTime toDate)
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
            if ((int?) payload["displayed_page"] == (int?) payload["total_pages"]) {
                return records;
            }
        }
    }
}