namespace Aestribor.VarAgent.Models;

/// <summary>Comando remoto enviado por el Hub de Aestribor para guardar el Replay Buffer.</summary>
public class SaveReplayCommand
{
    public Guid RequestId { get; set; }
    public int? CompetitionId { get; set; }
    public int? RaceId { get; set; }
    public DateTime RequestedAtUtc { get; set; }
}
