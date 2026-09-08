using Pz.Connector.Elasticsearch;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new EsConnector(ctx.LoggerFactory)).ConfigureAwait(false);
