namespace Aestribor.VarAgent.Models;

/// <summary>Resultado que el agente informa al Hub (método "CaptureResult") tras StartCapture/StopCapture.</summary>
public class CaptureResult
{
    public Guid RequestId { get; set; }
    public string StationId { get; set; } = string.Empty;
    public string Action { get; set; } = string.Empty;
    public bool Success { get; set; }
    public string Status { get; set; } = string.Empty;
    public string? ErrorMessage { get; set; }
    public string? RecordingPath { get; set; }
    public DateTime ProcessedAtUtc { get; set; }
}

public static class CaptureAction
{
    public const string Start = "START";
    public const string Stop = "STOP";
}

public static class CaptureStatus
{
    public const string Started = "STARTED";
    public const string Stopped = "STOPPED";
    public const string NoChange = "NO_CHANGE";
    public const string ObsDisconnected = "OBS_DISCONNECTED";
    public const string Error = "ERROR";
}
