using System.Text;
using Apache.Arrow.Types;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsSchemaMapperTests
{
    /// <summary>A mapping response exactly as GET _mapping answers it, through the client's own
    /// response deserializer, so the property types the mapper sees are the ones the real client
    /// produces.</summary>
    internal static IReadOnlyDictionary<string, IndexMappingRecord> Mappings(string json)
    {
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), new InMemoryRequestInvoker()));
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return client.RequestResponseSerializer.Deserialize<GetMappingResponse>(stream)!.Mappings;
    }

    private static EsDatasetConfig Dataset(params string[] jsonFields)
    {
        var errors = new List<string>();
        return EsDatasetConfig.Parse(new DatasetSpec("s", "d", new Dictionary<string, object?> { ["json_fields"] = jsonFields.Cast<object?>().ToList() }), errors)!;
    }

    private const string Wide = """
        {"idx":{"mappings":{"properties":{
          "b":{"type":"boolean"},
          "bin":{"type":"binary"},
          "by":{"type":"byte"},
          "d":{"type":"double"},
          "dt":{"type":"date"},
          "dt_custom":{"type":"date","format":"yyyy-MM-dd"},
          "dt_nanos":{"type":"date_nanos"},
          "dt_sec":{"type":"date","format":"epoch_second"},
          "dt_std":{"type":"date","format":"strict_date_optional_time||epoch_millis"},
          "f":{"type":"float"},
          "fl":{"type":"flattened"},
          "g":{"type":"geo_point"},
          "hf":{"type":"half_float"},
          "i":{"type":"integer"},
          "ip":{"type":"ip"},
          "k":{"type":"keyword"},
          "l":{"type":"long"},
          "n":{"type":"nested","properties":{"x":{"type":"integer"}}},
          "o":{"properties":{"city":{"type":"text","fields":{"raw":{"type":"keyword"}}},"geo":{"properties":{"lat":{"type":"double"}}}}},
          "o_empty":{"type":"object","enabled":false},
          "s":{"type":"short"},
          "sf":{"type":"scaled_float","scaling_factor":100},
          "t":{"type":"text"},
          "ul":{"type":"unsigned_long"},
          "v":{"type":"dense_vector","dims":3},
          "w":{"type":"some_future_type"},
          "z_alias":{"type":"alias","path":"k"}
        }}}}
        """;

    [Fact]
    public void Maps_every_type_flattens_objects_and_ends_with_id()
    {
        var plan = EsSchemaMapper.Plan(Mappings(Wide), Dataset(), "d", EsRedactor.None);

        var expected = new (string Name, ColumnKind Kind)[]
        {
            ("b", ColumnKind.Boolean), ("bin", ColumnKind.Text), ("by", ColumnKind.Int32), ("d", ColumnKind.Double),
            ("dt", ColumnKind.Timestamp), ("dt_custom", ColumnKind.Text), ("dt_nanos", ColumnKind.Timestamp),
            ("dt_sec", ColumnKind.Timestamp), ("dt_std", ColumnKind.Timestamp), ("f", ColumnKind.Double),
            ("fl", ColumnKind.Json), ("g", ColumnKind.Json), ("hf", ColumnKind.Double), ("i", ColumnKind.Int32),
            ("ip", ColumnKind.Text), ("k", ColumnKind.Text), ("l", ColumnKind.Int64), ("n", ColumnKind.Json),
            ("o.city", ColumnKind.Text), ("o.geo.lat", ColumnKind.Double), ("o_empty", ColumnKind.Json),
            ("s", ColumnKind.Int32), ("sf", ColumnKind.Double), ("t", ColumnKind.Text), ("ul", ColumnKind.Int64),
            ("v", ColumnKind.Json), ("w", ColumnKind.Json), ("_id", ColumnKind.Id),
        };
        Assert.Equal(expected.Select(e => e.Name), plan.Columns.Select(c => c.Name));
        Assert.Equal(expected.Select(e => e.Kind), plan.Columns.Select(c => c.Kind));
        Assert.Equal(EpochUnit.Seconds, plan.Columns.Single(c => c.Name == "dt_sec").Epoch);
        Assert.Equal(EpochUnit.Milliseconds, plan.Columns.Single(c => c.Name == "dt_std").Epoch);
        Assert.Equal(["o", "geo", "lat"], plan.Columns.Single(c => c.Name == "o.geo.lat").Path);
        Assert.DoesNotContain(plan.Columns, c => c.Name.Contains("raw") || c.Name == "z_alias" || c.Name == "n.x");
    }

    [Fact]
    public void Schema_types_follow_the_kinds_and_id_is_the_only_non_nullable_column()
    {
        var plan = EsSchemaMapper.Plan(Mappings(Wide), Dataset(), "d", EsRedactor.None);
        var fields = plan.Schema.FieldsList;

        Assert.Equal(ArrowTypeId.Boolean, fields[0].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Int32, fields[2].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Double, fields[3].DataType.TypeId);
        var ts = Assert.IsType<TimestampType>(fields[4].DataType);
        Assert.Equal(Apache.Arrow.Types.TimeUnit.Microsecond, ts.Unit);
        Assert.Equal("UTC", ts.Timezone);
        Assert.Equal(ArrowTypeId.String, fields[5].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Int64, fields[16].DataType.TypeId);
        Assert.All(fields.SkipLast(1), f => Assert.True(f.IsNullable));
        Assert.False(fields[^1].IsNullable);
        Assert.Equal("_id", fields[^1].Name);
    }

    [Fact]
    public void Json_fields_stop_flattening_and_force_json_text()
    {
        var plan = EsSchemaMapper.Plan(Mappings(Wide), Dataset("o", "k", "o.geo.lat"), "d", EsRedactor.None);

        Assert.Equal(ColumnKind.Json, plan.Columns.Single(c => c.Name == "o").Kind);
        Assert.Equal(ColumnKind.Json, plan.Columns.Single(c => c.Name == "k").Kind);
        Assert.DoesNotContain(plan.Columns, c => c.Name.StartsWith("o.", StringComparison.Ordinal));
    }

    [Fact]
    public void An_empty_index_has_only_the_id_column()
    {
        var plan = EsSchemaMapper.Plan(Mappings("""{"idx":{"mappings":{}}}"""), Dataset(), "d", EsRedactor.None);

        Assert.Equal(["_id"], plan.Columns.Select(c => c.Name));
        Assert.Empty(plan.SourcePaths);
    }

    [Fact]
    public void Indices_behind_a_pattern_merge_when_they_agree_and_refuse_when_they_do_not()
    {
        var agree = Mappings("""{"a":{"mappings":{"properties":{"id":{"type":"long"},"x":{"type":"keyword"}}}},"b":{"mappings":{"properties":{"id":{"type":"long"},"y":{"type":"boolean"}}}}}""");
        var plan = EsSchemaMapper.Plan(agree, Dataset(), "d", EsRedactor.None);
        Assert.Equal(["id", "x", "y", "_id"], plan.Columns.Select(c => c.Name));

        var disagree = Mappings("""{"a":{"mappings":{"properties":{"id":{"type":"long"}}}},"b":{"mappings":{"properties":{"id":{"type":"keyword"}}}}}""");
        var ex = Assert.Throws<PzConnectorException>(() => EsSchemaMapper.Plan(disagree, Dataset(), "d", EsRedactor.None));
        Assert.False(ex.IsTransient);
        Assert.Equal("elasticsearch: dataset 'd': field 'id' is mapped as 'long' in one index and 'keyword' in 'b'; 'index:' spans "
            + "indices whose mappings disagree, so narrow it or list the field under json_fields:", ex.Message);
    }

    [Fact]
    public void Project_keeps_plan_order_and_returns_the_full_plan_for_an_unknown_name()
    {
        var plan = EsSchemaMapper.Plan(Mappings(Wide), Dataset(), "d", EsRedactor.None);

        var projected = plan.Project(["_id", "o.city", "b"]);
        Assert.Equal(["b", "o.city", "_id"], projected.Columns.Select(c => c.Name));
        Assert.Equal(["b", "o.city", "_id"], projected.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(["b", "o.city"], projected.SourcePaths);

        Assert.Same(plan, plan.Project(["b", "nope"]));
        Assert.Same(plan, plan.Project(null));
        Assert.Same(plan, plan.Project([]));
    }
}
