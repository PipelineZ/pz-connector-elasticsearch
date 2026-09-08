using System.Text.Json;
using System.Text.Json.Serialization;

namespace Pz.Connector.Elasticsearch;

/// <summary>The source-document serializer context handed to the Elastic client. Documents only
/// ever travel as <see cref="JsonElement"/>; a reflection-based resolver has no metadata under
/// Native AOT, so this is the whole set of document types the client may see.</summary>
[JsonSerializable(typeof(JsonElement))]
internal sealed partial class EsJsonContext : JsonSerializerContext;
