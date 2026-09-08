using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsConnectorTests
{
    [Fact]
    public void Info_names_the_connector_and_the_protocol()
    {
        var connector = new EsConnector();
        Assert.Equal("elasticsearch", connector.Info.Name);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
    }
}
