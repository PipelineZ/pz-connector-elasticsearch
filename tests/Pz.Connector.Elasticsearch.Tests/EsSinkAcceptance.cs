using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Elasticsearch.Tests;

/// <summary>TestKit sink contract. The suite writes a fixed (id Int64, name String) schema; the
/// connector indexes each row as {"id":..,"name":..}, so read-back parses the documents into the
/// same two columns. Fresh names per test-class instance (xunit instantiates per fact), so facts
/// never see each other's documents: an index for append, an index for merge, an alias name for
/// replace.</summary>
[Collection("elasticsearch")]
[Trait("Category", "Docker")]
public sealed class EsSinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly EsFixture _es;
    private readonly string _append = EsFixture.NewName("append");
    private readonly string _merge = EsFixture.NewName("merge");
    private readonly string _replace = EsFixture.NewName("replace");

    public EsSinkAcceptance(EsFixture es)
    {
        _es = es;
        DockerFacts.SkipUnlessDocker();
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new EsConnector();

    protected override ConnectorConfig ValidConfig => new(_es.ConnectionConfig());

    protected override OutputSpec SmallOutput => new("elasticsearch", _append, "append", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput =>
        new OutputSpec("elasticsearch", _merge, "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override Task ResetMergeTargetAsync() => _es.DeleteIndexAsync(_merge);

    protected override OutputSpec? ReplaceOutput => new("elasticsearch", _replace, "replace", "fail_on_change", new Dictionary<string, object?>());

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        if (!await _es.IndexExistsAsync(spec.Output))
        {
            return [];
        }

        var docs = await _es.SearchAllAsync(spec.Output);
        if (docs.Count == 0)
        {
            return [];
        }

        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (_, source) in docs)
        {
            ids.Append(source.GetProperty("id").GetInt64());
            names.Append(source.GetProperty("name").GetString()!);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);
        return [new RecordBatch(schema, [ids.Build(), names.Build()], docs.Count)];
    }
}
