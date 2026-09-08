using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Elasticsearch.Tests;

/// <summary>TestKit source contract against the Testcontainers node. SmallDataset is an index of 120
/// ~100-byte documents (>= 100 rows, >= 2 batches under the suite's 4KB target) whose first column
/// is the long <c>id</c> the inclusive-watermark fact treats as the cursor. LargeDataset is 150k
/// documents so mid-read cancellation is observable. BoundedWindowDataset seeds ids 0..10. All
/// three indices are created once per fixture on first use.</summary>
[Collection("elasticsearch")]
[Trait("Category", "Docker")]
public sealed class EsSourceAcceptance : SourceConnectorAcceptanceTests
{
    private static readonly SemaphoreSlim Seed = new(1, 1);
    private static string? _small;
    private static string? _large;
    private static string? _window;
    private readonly EsFixture _es;

    public EsSourceAcceptance(EsFixture es)
    {
        _es = es;
        DockerFacts.SkipUnlessDocker();
        SeedAsync().GetAwaiter().GetResult();
    }

    protected override ConnectorConfig ValidConfig => new(_es.ConnectionConfig());

    protected override DatasetSpec SmallDataset => new("elasticsearch", _small!, new Dictionary<string, object?>());

    protected override DatasetSpec? LargeDataset => new("elasticsearch", _large!, new Dictionary<string, object?>());

    protected override DatasetSpec? BoundedWindowDataset =>
        new DatasetSpec("elasticsearch", _window!, new Dictionary<string, object?>())
        {
            WatermarkCursor = "id", WatermarkValue = "3", WatermarkUpperBound = "7",
        };

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new EsConnector();

    private async Task SeedAsync()
    {
        await Seed.WaitAsync();
        try
        {
            _small ??= await SeedIndexAsync(120);
            _large ??= await SeedIndexAsync(150_000);
            _window ??= await SeedIndexAsync(11);
        }
        finally
        {
            Seed.Release();
        }
    }

    private async Task<string> SeedIndexAsync(int rows)
    {
        var index = await _es.CreateIndexAsync(EsFixture.RowsMapping);
        await _es.BulkAsync(index, EsFixture.Rows(rows));
        return index;
    }
}
