using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Pz.Connectors.Abstractions;
using Testcontainers.Elasticsearch;

namespace Pz.Connector.Elasticsearch.Tests;

/// <summary>The security surface against the image's default posture: TLS with a self-signed
/// HTTP CA and the <c>elastic</c> user. Proves each trust setting (<c>insecure</c>, <c>ca_cert</c>,
/// <c>ca_fingerprint</c>) and each credential kind (basic, api_key), plus the 401 text a wrong
/// credential produces. Its own container: the shared fixture runs with security off.</summary>
[Collection("elasticsearch-secured")]
[Trait("Category", "Docker")]
public sealed class EsSecuredFacts(EsSecuredFixture node)
{
    private string _url => node.Url;
    private string _caPem => node.CaPem;

    private Dictionary<string, object?> Config(params (string Key, object? Value)[] extra)
    {
        var config = new Dictionary<string, object?> { ["url"] = _url, ["timeout"] = 30L };
        foreach (var (key, value) in extra)
        {
            config[key] = value;
        }

        return config;
    }

    private static Task<ConnectionCheck> CheckAsync(Dictionary<string, object?> config) =>
        new EsConnector().CheckConnectionAsync(new ConnectorConfig(config), CancellationToken.None).AsTask();

    [SkippableFact]
    public async Task Basic_auth_over_tls_with_insecure_connects()
    {
        DockerFacts.SkipUnlessDocker();
        var check = await CheckAsync(Config(("username", "elastic"), ("password", "elastic"), ("insecure", true)));

        Assert.True(check.Ok, check.Message);
        Assert.Contains("docker-cluster", check.Message);
    }

    [SkippableFact]
    public async Task Api_key_authenticates()
    {
        DockerFacts.SkipUnlessDocker();
        using var handler = new HttpClientHandler { ServerCertificateCustomValidationCallback = (_, _, _, _) => true };
        using var http = new HttpClient(handler);
        using var request = new HttpRequestMessage(HttpMethod.Post, _url + "/_security/api_key")
        {
            Content = new StringContent("""{"name":"pz-test"}""", Encoding.UTF8, "application/json"),
        };
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String("elastic:elastic"u8.ToArray()));
        using var response = await http.SendAsync(request);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var encoded = doc.RootElement.GetProperty("encoded").GetString()!;

        var check = await CheckAsync(Config(("api_key", encoded), ("insecure", true)));

        Assert.True(check.Ok, check.Message);
    }

    [SkippableFact]
    public async Task Ca_cert_and_ca_fingerprint_each_trust_the_node()
    {
        DockerFacts.SkipUnlessDocker();
        var dir = Directory.CreateTempSubdirectory("pz-es-");
        try
        {
            await File.WriteAllTextAsync(Path.Combine(dir.FullName, "http_ca.crt"), _caPem);
            var byFile = await CheckAsync(Config(("username", "elastic"), ("password", "elastic"), ("ca_cert", "http_ca.crt"), ("base_dir", dir.FullName)));
            Assert.True(byFile.Ok, byFile.Message);

            using var ca = X509CertificateLoader.LoadCertificate(Encoding.UTF8.GetBytes(_caPem));
            var fingerprint = Convert.ToHexString(SHA256.HashData(ca.RawData));
            var byFingerprint = await CheckAsync(Config(("username", "elastic"), ("password", "elastic"), ("ca_fingerprint", fingerprint)));
            Assert.True(byFingerprint.Ok, byFingerprint.Message);
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }

    [SkippableFact]
    public async Task An_untrusted_certificate_is_a_failed_probe_not_a_crash()
    {
        DockerFacts.SkipUnlessDocker();
        var check = await CheckAsync(Config(("username", "elastic"), ("password", "elastic")));

        Assert.False(check.Ok);
        Assert.StartsWith("elasticsearch: checking the connection: ", check.Message);
    }

    [SkippableFact]
    public async Task A_wrong_password_reports_401_with_the_hint_and_without_the_password()
    {
        DockerFacts.SkipUnlessDocker();
        var check = await CheckAsync(Config(("username", "elastic"), ("password", "wrong-secret-1"), ("insecure", true)));

        Assert.False(check.Ok);
        Assert.Contains("security_exception", check.Message);
        Assert.Contains("(HTTP 401; check api_key, or username and password)", check.Message);
        Assert.DoesNotContain("wrong-secret-1", check.Message);
    }

    [SkippableFact]
    public async Task Reading_and_writing_work_over_the_secured_node()
    {
        DockerFacts.SkipUnlessDocker();
        var config = new ConnectorConfig(Config(("username", "elastic"), ("password", "elastic"), ("insecure", true)));
        var index = EsFixture.NewName("secured");
        var schema = new Apache.Arrow.Schema([new Apache.Arrow.Field("id", Apache.Arrow.Types.Int64Type.Default, true)], null);

        ISinkConnector sinkConnector = new EsConnector();
        await using (var sink = await sinkConnector.OpenAsync(config, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(new OutputSpec("e", index, "append", "fail_on_change", new Dictionary<string, object?>()), schema, CancellationToken.None))
        {
            using var batch = new Apache.Arrow.RecordBatch(schema, [new Apache.Arrow.Int64Array.Builder().Append(1).Append(2).Build()], 2);
            await session.WriteBatchAsync(batch, CancellationToken.None);
            Assert.Equal(2, (await session.CommitAsync(CancellationToken.None)).RowsWritten);
        }

        ISourceConnector sourceConnector = new EsConnector();
        await using var source = await sourceConnector.OpenAsync(config, CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("e", index, new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
        var rows = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        Assert.Equal(2, rows);
    }
}

[CollectionDefinition("elasticsearch-secured")]
public sealed class EsSecuredCollection : ICollectionFixture<EsSecuredFixture>;

/// <summary>One secured node per test run (the image's defaults: TLS, <c>elastic</c>/<c>elastic</c>),
/// with its HTTP CA read out of the container for the trust facts.</summary>
public sealed class EsSecuredFixture : IAsyncLifetime
{
    private ElasticsearchContainer? _container;

    public string Url { get; private set; } = "";

    public string CaPem { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        _container = new ElasticsearchBuilder(EsFixture.Image).Build();
        await _container.StartAsync();
        var cs = new Uri(_container.GetConnectionString());
        Url = $"{cs.Scheme}://{cs.Host}:{cs.Port}";
        CaPem = Encoding.UTF8.GetString(await _container.ReadFileAsync("/usr/share/elasticsearch/config/certs/http_ca.crt"));
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }
}
