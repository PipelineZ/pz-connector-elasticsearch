using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Pz.Connector.Elasticsearch.Tests;

[CollectionDefinition("elasticsearch")]
public sealed class EsCollection : ICollectionFixture<EsFixture>;

/// <summary>One single-node Elasticsearch 9.5.3 per test run, security off, plain HTTP. Index
/// names are unique per call so facts never share state. Helpers talk to the node with a plain
/// HttpClient, deliberately not through the connector: a fact that used the code under test to
/// seed and verify would prove nothing.</summary>
public sealed class EsFixture : IAsyncLifetime
{
    public const string Image = "elasticsearch:9.5.3";

    private IContainer? _container;
    private HttpClient? _http;

    public string Url { get; private set; } = "";

    public HttpClient Http => _http ?? throw new InvalidOperationException("the node is not running");

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        // Built here rather than in a field initializer: Build() resolves and pings the docker
        // endpoint, so a constructor that built it would throw before the probe above could no-op --
        // and a collection fixture that throws is a failed fixture, not a skip.
        _container = new ContainerBuilder(Image)
            .WithEnvironment("discovery.type", "single-node")
            .WithEnvironment("xpack.security.enabled", "false")
            .WithEnvironment("ES_JAVA_OPTS", "-Xms1g -Xmx1g")
            .WithPortBinding(9200, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(9200).ForPath("/_cluster/health")))
            .Build();
        await _container.StartAsync().ConfigureAwait(false);
        Url = $"http://{_container.Hostname}:{_container.GetMappedPublicPort(9200)}";
        _http = new HttpClient { BaseAddress = new Uri(Url), Timeout = TimeSpan.FromMinutes(2) };
    }

    public async Task DisposeAsync()
    {
        _http?.Dispose();
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Dictionary<string, object?> ConnectionConfig() => new() { ["url"] = Url };

    public static string NewName(string prefix = "pz") => $"{prefix}-{Guid.NewGuid():N}"[..(prefix.Length + 13)];

    /// <summary>Creates an index with the given <c>properties</c> mapping JSON (null = dynamic).</summary>
    public async Task<string> CreateIndexAsync(string? propertiesJson, string? name = null)
    {
        name ??= NewName();
        var body = propertiesJson is null ? "{}" : "{\"mappings\":{\"properties\":{" + propertiesJson + "}}}";
        await SendAsync(HttpMethod.Put, $"/{name}", body).ConfigureAwait(false);
        return name;
    }

    /// <summary>Indexes documents in bulks of 1000 with <c>refresh</c> after the last one.</summary>
    public async Task BulkAsync(string index, IEnumerable<(string? Id, string Json)> docs)
    {
        var sb = new StringBuilder();
        var count = 0;
        foreach (var (id, json) in docs)
        {
            sb.Append(id is null ? "{\"index\":{}}\n" : $"{{\"index\":{{\"_id\":{JsonSerializer.Serialize(id)}}}}}\n").Append(json).Append('\n');
            if (++count % 1000 == 0)
            {
                await SendAsync(HttpMethod.Post, $"/{index}/_bulk", sb.ToString(), ndjson: true).ConfigureAwait(false);
                sb.Clear();
            }
        }

        if (sb.Length > 0)
        {
            await SendAsync(HttpMethod.Post, $"/{index}/_bulk", sb.ToString(), ndjson: true).ConfigureAwait(false);
        }

        await SendAsync(HttpMethod.Post, $"/{index}/_refresh", null).ConfigureAwait(false);
    }

    /// <summary>Every document in the index (up to 10000), as (id, source).</summary>
    public async Task<List<(string Id, JsonElement Source)>> SearchAllAsync(string index)
    {
        var text = await SendAsync(HttpMethod.Post, $"/{index}/_search?size=10000", """{"query":{"match_all":{}}}""").ConfigureAwait(false);
        using var doc = JsonDocument.Parse(text);
        return doc.RootElement.GetProperty("hits").GetProperty("hits").EnumerateArray()
            .Select(h => (h.GetProperty("_id").GetString()!, h.GetProperty("_source").Clone())).ToList();
    }

    public async Task<long> CountAsync(string index)
    {
        var text = await SendAsync(HttpMethod.Get, $"/{index}/_count", null).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(text);
        return doc.RootElement.GetProperty("count").GetInt64();
    }

    /// <summary>The indices behind an alias, sorted; empty when the alias does not exist.</summary>
    public async Task<List<string>> AliasIndicesAsync(string alias)
    {
        using var response = await Http.GetAsync($"/_alias/{alias}").ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return [];
        }

        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        using var doc = JsonDocument.Parse(text);
        return doc.RootElement.EnumerateObject().Select(p => p.Name).Order(StringComparer.Ordinal).ToList();
    }

    public async Task<bool> IndexExistsAsync(string index)
    {
        using var response = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Head, $"/{index}")).ConfigureAwait(false);
        return response.IsSuccessStatusCode;
    }

    public async Task<JsonElement> GetMappingPropertiesAsync(string index)
    {
        var text = await SendAsync(HttpMethod.Get, $"/{index}/_mapping", null).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(text);
        var mappings = doc.RootElement.EnumerateObject().First().Value.GetProperty("mappings");
        return mappings.TryGetProperty("properties", out var props) ? props.Clone() : JsonDocument.Parse("{}").RootElement.Clone();
    }

    public Task AddAliasAsync(string index, string alias) =>
        SendAsync(HttpMethod.Post, "/_aliases", "{\"actions\":[{\"add\":{\"index\":\"" + index + "\",\"alias\":\"" + alias + "\",\"is_write_index\":true}}]}");

    public async Task DeleteIndexAsync(string index)
    {
        using var response = await Http.DeleteAsync($"/{index}").ConfigureAwait(false);
    }

    public async Task<string> SendAsync(HttpMethod method, string path, string? body, bool ndjson = false)
    {
        using var request = new HttpRequestMessage(method, path);
        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8);
            request.Content.Headers.ContentType = new MediaTypeHeaderValue(ndjson ? "application/x-ndjson" : "application/json");
        }

        using var response = await Http.SendAsync(request).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"{method} {path} -> {(int)response.StatusCode}: {text}");
        }

        return text;
    }

    /// <summary>Documents <c>{id, name, pad}</c> for <c>id</c> in [0, rows).</summary>
    public static IEnumerable<(string? Id, string Json)> Rows(int rows, string namePrefix = "n") =>
        Enumerable.Range(0, rows).Select(i => ((string?)$"d{i}", "{\"id\":" + i + ",\"name\":\"" + namePrefix + i + "\",\"pad\":\"" + new string('x', 80) + "\"}"));

    public const string RowsMapping = "\"id\":{\"type\":\"long\"},\"name\":{\"type\":\"keyword\"},\"pad\":{\"type\":\"keyword\"}";
}
