using Pz.Connectors.TestKit;

namespace Pz.Connector.Elasticsearch.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config seeds it with the api key.</summary>
public sealed class EsRedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new EsRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}
