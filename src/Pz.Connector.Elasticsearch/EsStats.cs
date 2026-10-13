using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Maps what Elasticsearch itself reports about a call onto the node's remote stats. Only
/// server-reported figures are used (<c>took</c> is the time the cluster spent on the request), and
/// reporting never fails a call: a body of an unexpected shape reports nothing.</summary>
internal static class EsStats
{
    /// <summary>Reads <c>took</c> and <c>_shards.failed</c> off a search response root. The HTTP
    /// handler already counts the request and its bytes.</summary>
    public static void ReportSearch(JsonElement root, IConnectorStats stats)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        if (root.TryGetProperty("took", out var took) && took.ValueKind == JsonValueKind.Number && took.TryGetInt64(out var ms) && ms >= 0)
        {
            stats.AddRemoteMs(ms);
        }

        if (root.TryGetProperty("_shards", out var shards) && shards.ValueKind == JsonValueKind.Object
            && shards.TryGetProperty("failed", out var failed) && failed.ValueKind == JsonValueKind.Number
            && failed.TryGetInt64(out var n) && n > 0)
        {
            stats.AddExtra("elasticsearch.shards_failed", n);
        }
    }

    /// <summary>Reads <c>took</c>, per-item 429 rejections (the bulk queue pushing back inside a 200
    /// envelope, which the HTTP handler cannot see) and the count of rejected items off a bulk
    /// response.</summary>
    public static void ReportBulk(BulkResponse response, IConnectorStats stats)
    {
        if (response.Took >= 0)
        {
            stats.AddRemoteMs(response.Took);
        }

        long failed = 0;
        long throttled = 0;
        foreach (var item in response.ItemsWithErrors)
        {
            failed++;
            if (item.Status == 429)
            {
                throttled++;
            }
        }

        if (throttled > 0)
        {
            stats.AddThrottled(throttled);
        }

        if (failed > 0)
        {
            stats.AddExtra("elasticsearch.bulk_items_failed", failed);
        }
    }
}
