namespace Aestribor.VarAgent.Models;

/// <summary>Confirmación que el agente envía al Hub (método "ReplayResult") tras procesar un SaveReplay.</summary>
public class ReplayResult
{
    public Guid RequestId { get; set; }
    public string StationId { get; set; } = string.Empty;
    public bool Success { get; set; }
    public string Status { get; set; } = string.Empty;
    public string? ErrorMessage { get; set; }
    public DateTime ProcessedAtUtc { get; set; }
}

public static class ReplayStatus
{
    public const string Saved = "SAVED";
    public const string BufferInactive = "BUFFER_INACTIVE";
    public const string ObsDisconnected = "OBS_DISCONNECTED";
    public const string Error = "ERROR";
}
