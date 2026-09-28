namespace Aestribor.VarAgent;

public sealed class AestriborSettings
{
    public string SignalRHubUrl { get; set; } = string.Empty;
    public string StationId { get; set; } = string.Empty;
    public string ApiKey { get; set; } = string.Empty;
}

public sealed class ObsSettings
{
    public string Url { get; set; } = "ws://127.0.0.1:4455";
    public string Password { get; set; } = string.Empty;
    public int ConnectTimeoutSeconds { get; set; } = 5;
    public int ReconnectDelaySeconds { get; set; } = 5;
}
