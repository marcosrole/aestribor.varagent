namespace Aestribor.VarAgent.Models;

/// <summary>
/// Comando remoto para iniciar ("StartCapture") o detener ("StopCapture") la grabación
/// y el Replay Buffer de OBS. Lo envía el servidor al iniciar/finalizar heats.
/// </summary>
public class CaptureCommand
{
    public Guid RequestId { get; set; }
    public int? CompetitionId { get; set; }
    public List<int> RaceIds { get; set; } = new();
    public DateTime RequestedAtUtc { get; set; }
}
