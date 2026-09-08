using System.Security.Cryptography.X509Certificates;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Serialization;
using Elastic.Transport;

namespace Pz.Connector.Elasticsearch;

/// <summary>Builds the client every source and sink talks through. The engine owns retries and
/// pacing, so the client never retries or pings on its own; failures come back as invalid
/// responses (never thrown) and are classified by <see cref="EsErrors"/>. Documents are only ever
/// <see cref="System.Text.Json.JsonElement"/>, which is the whole source-serializer contract under
/// Native AOT.</summary>
internal static class EsClientFactory
{
    public static ElasticsearchClient Create(EsConnectionConfig config)
    {
        var settings = new ElasticsearchClientSettings(new SingleNodePool(config.Url), new HttpRequestInvoker(),
                (_, s) => new DefaultSourceSerializer(s, EsJsonContext.Default, _ => { }))
            .MaximumRetries(0)
            .DisablePing()
            .ThrowExceptions(false)
            .RequestTimeout(TimeSpan.FromSeconds(config.TimeoutSeconds));

        switch (config.Auth.Kind)
        {
            case EsAuthKind.ApiKey:
                settings.Authentication(new ApiKey(config.Auth.ApiKey!));
                break;
            case EsAuthKind.Basic:
                settings.Authentication(new BasicAuthentication(config.Auth.Username!, config.Auth.Password!));
                break;
        }

        if (config.Insecure)
        {
            settings.ServerCertificateValidationCallback(CertificateValidations.AllowAll);
        }
        else if (config.CaFingerprint is not null)
        {
            settings.CertificateFingerprint(config.CaFingerprint);
        }
        else if (config.CaCertPath is not null)
        {
            // Loaded once here rather than inside the callback: a missing or unreadable file is a
            // configuration error the open should report, not something to rediscover per request.
            var ca = X509CertificateLoader.LoadCertificateFromFile(config.CaCertPath);
            settings.ServerCertificateValidationCallback((_, certificate, _, _) => TrustedByCustomRoot(certificate, ca));
        }

        return new ElasticsearchClient(settings);
    }

    /// <summary>Validates the server chain against exactly the configured CA, ignoring the machine
    /// trust store: the option means "this CA, not whatever the host trusts".</summary>
    private static bool TrustedByCustomRoot(X509Certificate? certificate, X509Certificate2 ca)
    {
        if (certificate is null)
        {
            return false;
        }

        using var leaf = new X509Certificate2(certificate);
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(leaf);
    }
}
