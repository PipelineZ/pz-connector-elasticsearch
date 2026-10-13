using System.Diagnostics;
using System.Net;
using System.Text;
using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsStatsTests
{
    private static JsonElement Json(string text)
    {
        using var doc = JsonDocument.Parse(text);
        return doc.RootElement.Clone();
    }

    private static BulkResponse Bulk(string text)
    {
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200"))));
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(text));
        return client.RequestResponseSerializer.Deserialize<BulkResponse>(stream)!;
    }

    [Fact]
    public void Search_took_is_remote_time_and_failed_shards_are_an_extra()
    {
        var acc = new ConnectorStatsAccumulator();
        EsStats.ReportSearch(Json("""{"took":12,"_shards":{"total":3,"successful":2,"failed":1},"hits":{"hits":[]}}"""), acc);

        var s = acc.Snapshot();
        Assert.Equal(12, s.RemoteMs);
        Assert.Contains(new KeyValuePair<string, double>("elasticsearch.shards_failed", 1), s.Extras);
    }

    [Fact]
    public void Search_without_failed_shards_adds_no_extra()
    {
        var acc = new ConnectorStatsAccumulator();
        EsStats.ReportSearch(Json("""{"took":3,"_shards":{"total":3,"successful":3,"failed":0}}"""), acc);

        var s = acc.Snapshot();
        Assert.Equal(3, s.RemoteMs);
        Assert.Empty(s.Extras);
    }

    [Theory]
    [InlineData("""[1,2]""")]
    [InlineData("""{"took":"soon","_shards":[1]}""")]
    [InlineData("""{"took":-4,"_shards":{"failed":"x"}}""")]
    [InlineData("""{}""")]
    public void A_search_body_of_an_unexpected_shape_reports_nothing_and_never_throws(string body)
    {
        var acc = new ConnectorStatsAccumulator();
        EsStats.ReportSearch(Json(body), acc);
        Assert.True(acc.Snapshot().IsEmpty);
    }

    [Fact]
    public void Bulk_items_rejected_with_429_are_throttled_and_every_rejected_item_is_counted()
    {
        var acc = new ConnectorStatsAccumulator();
        var response = Bulk("""
            {"took":7,"errors":true,"items":[
              {"index":{"_index":"i","_id":"1","status":429,"error":{"type":"es_rejected_execution_exception","reason":"queue full"}}},
              {"index":{"_index":"i","_id":"2","status":429,"error":{"type":"es_rejected_execution_exception","reason":"queue full"}}},
              {"index":{"_index":"i","_id":"3","status":201,"result":"created"}}]}
            """);

        EsStats.ReportBulk(response, acc);

        var s = acc.Snapshot();
        Assert.Equal(2, s.Throttled);
        Assert.Equal(7, s.RemoteMs);
        Assert.Contains(new KeyValuePair<string, double>("elasticsearch.bulk_items_failed", 2), s.Extras);
    }

    [Fact]
    public void Bulk_with_a_non_throttle_failure_counts_it_as_failed_only()
    {
        var acc = new ConnectorStatsAccumulator();
        EsStats.ReportBulk(Bulk("""
            {"took":1,"errors":true,"items":[
              {"index":{"_index":"i","_id":"1","status":400,"error":{"type":"mapper_parsing_exception","reason":"bad"}}}]}
            """), acc);

        var s = acc.Snapshot();
        Assert.Equal(0, s.Throttled);
        Assert.Contains(new KeyValuePair<string, double>("elasticsearch.bulk_items_failed", 1), s.Extras);
    }

    [Fact]
    public void A_clean_bulk_reports_only_time()
    {
        var acc = new ConnectorStatsAccumulator();
        EsStats.ReportBulk(Bulk("""{"took":5,"errors":false,"items":[{"index":{"_index":"i","_id":"1","status":201}}]}"""), acc);

        var s = acc.Snapshot();
        Assert.Equal(5, s.RemoteMs);
        Assert.Equal(0, s.Throttled);
        Assert.Empty(s.Extras);
    }

    [Fact]
    public void A_bulk_body_that_does_not_describe_a_response_reports_nothing_and_never_throws()
    {
        var acc = new ConnectorStatsAccumulator();
        EsStats.ReportBulk(Bulk("{}"), acc);
        Assert.True(acc.Snapshot().IsEmpty);
    }

    [Fact]
    public async Task A_429_through_the_client_counts_one_request_and_one_throttle()
    {
        var acc = new ConnectorStatsAccumulator();
        var remote = new RemoteCalls(new ActivitySource("test-es"));
        var config = EsConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?> { ["url"] = "http://localhost:9200" }), []);
        var client = EsClientFactory.Create(config!, remote, _ => new Fake(HttpStatusCode.TooManyRequests));

        using (ConnectorStats.Use(acc))
        {
            var info = await client.InfoAsync();
            Assert.False(info.IsValidResponse);
        }

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(1, s.Throttled);
    }

    [Fact]
    public async Task A_200_through_the_client_counts_one_request_and_no_throttle()
    {
        var acc = new ConnectorStatsAccumulator();
        var remote = new RemoteCalls(new ActivitySource("test-es"));
        var config = EsConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?> { ["url"] = "http://localhost:9200" }), []);
        var client = EsClientFactory.Create(config!, remote, _ => new Fake(HttpStatusCode.OK));

        using (ConnectorStats.Use(acc))
        {
            await client.InfoAsync();
        }

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Equal(0, s.Throttled);
    }

    [Fact]
    public void The_connector_declares_that_it_reports_stats()
    {
        Assert.True(new EsConnector().Capabilities.HasFlag(ConnectorCapabilities.ReportsStats));
    }

    private sealed class Fake(HttpStatusCode status) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // The client refuses a 2xx that does not identify itself as Elasticsearch.
            var response = new HttpResponseMessage(status) { Content = new StringContent("{}", Encoding.UTF8, "application/json") };
            response.Headers.TryAddWithoutValidation("X-Elastic-Product", "Elasticsearch");
            return Task.FromResult(response);
        }
    }
}
