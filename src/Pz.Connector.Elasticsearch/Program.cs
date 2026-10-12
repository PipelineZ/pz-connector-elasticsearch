using Pz.Connector.Elasticsearch;
using Pz.Connectors.Sdk;

// "Elastic.Transport" is the ActivitySource the Elastic client really emits on (pinned by
// EsActivitySourceTests); the SDK strips url.full and other user-data tags from it before export.
return await PzConnectorHost.RunAsync(
    args,
    ctx => new EsConnector(ctx.LoggerFactory, ctx.Remote),
    new PzConnectorHostOptions { ActivitySources = ["Elastic.Transport"] }).ConfigureAwait(false);
