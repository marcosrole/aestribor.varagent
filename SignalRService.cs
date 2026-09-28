using Aestribor.VarAgent.Models;
using Microsoft.AspNetCore.SignalR.Client;

namespace Aestribor.VarAgent;

/// <summary>
/// Conexión saliente al Hub SignalR de Aestribor. Recibe comandos remotos y los delega en ObsService.
/// No contiene lógica de OBS: solo traduce comando -> ObsService.SaveReplay() -> ReplayResult.
/// </summary>
public sealed class SignalRService : IAsyncDisposable
{
    // Contrato con el Hub (nombres de métodos acordados con el backend).
    private const string SaveReplayMethod = "SaveReplay";
    private const string ReplayResultMethod = "ReplayResult";
    private const string RegisterStationMethod = "RegisterStation";
    private const string ApiKeyHeader = "X-Api-Key";

    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(5);

    private readonly AestriborSettings _settings;
    private readonly ObsService _obs;
    private readonly HubConnection _connection;
    private readonly object _startLock = new();

    private CancellationTokenSource? _cts;
    private Task? _startLoop;
    private volatile bool _stopping;

    public SignalRService(AestriborSettings settings, ObsService obs)
    {
        _settings = settings;
        _obs = obs;

        _connection = new HubConnectionBuilder()
            .WithUrl(settings.SignalRHubUrl, options =>
            {
                if (!string.IsNullOrWhiteSpace(settings.ApiKey))
                {
                    options.Headers[ApiKeyHeader] = settings.ApiKey;
                }
            })
            .WithAutomaticReconnect(new ForeverRetryPolicy())
            .Build();

        _connection.On<SaveReplayCommand>(SaveReplayMethod, OnSaveReplayAsync);

        _connection.Reconnecting += OnReconnecting;
        _connection.Reconnected += OnReconnected;
        _connection.Closed += OnClosed;
    }

    public HubConnectionState State => _connection.State;

    /// <summary>
    /// Inicia la conexión en segundo plano. No bloquea: si el servidor no está disponible,
    /// reintenta hasta lograrlo o hasta que se cancele el token.
    /// </summary>
    public void Start(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        EnsureStartLoop();
    }

    public async Task StopAsync()
    {
        _stopping = true;
        _cts?.Cancel();

        if (_startLoop != null)
        {
            try { await _startLoop; } catch (OperationCanceledException) { }
        }

        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await _connection.StopAsync(timeout.Token);
        }
        catch (Exception ex)
        {
            ConsoleLog.Warn($"SignalR: error al cerrar la conexión: {ex.Message}");
        }
    }

    private void EnsureStartLoop()
    {
        lock (_startLock)
        {
            if (_stopping || _cts is null || (_startLoop is { IsCompleted: false }))
            {
                return;
            }

            _startLoop = Task.Run(() => StartLoopAsync(_cts.Token));
        }
    }

    // WithAutomaticReconnect solo cubre caídas posteriores a una conexión exitosa;
    // el primer StartAsync (o un reinicio tras Closed) se reintenta acá.
    private async Task StartLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                ConsoleLog.Info($"SignalR: conectando a {_settings.SignalRHubUrl} ...");
                await _connection.StartAsync(ct);
                ConsoleLog.Success($"SignalR: conectado (ConnectionId {_connection.ConnectionId}).");
                await RegisterStationAsync(ct);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                ConsoleLog.Error($"SignalR: no se pudo conectar: {ex.GetBaseException().Message}");
                ConsoleLog.Warn($"SignalR: reintentando en {InitialRetryDelay.TotalSeconds:0} s...");
            }

            try { await Task.Delay(InitialRetryDelay, ct); }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task RegisterStationAsync(CancellationToken ct)
    {
        try
        {
            await _connection.InvokeAsync(RegisterStationMethod, _settings.StationId, ct);
            ConsoleLog.Success($"SignalR: estación '{_settings.StationId}' registrada.");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // No es fatal: la conexión sigue viva y el agente puede recibir comandos.
            ConsoleLog.Error($"SignalR: no se pudo registrar la estación '{_settings.StationId}': {ex.Message}");
        }
    }

    private async Task OnSaveReplayAsync(SaveReplayCommand? command)
    {
        if (command is null)
        {
            ConsoleLog.Error("SignalR: se recibió SaveReplay sin datos; se ignora.");
            return;
        }

        ConsoleLog.Info($"SignalR: comando SaveReplay recibido. RequestId={command.RequestId}" +
                        $" CompetitionId={command.CompetitionId?.ToString() ?? "-"}" +
                        $" RaceId={command.RaceId?.ToString() ?? "-"}");

        var result = new ReplayResult
        {
            RequestId = command.RequestId,
            StationId = _settings.StationId
        };

        try
        {
            // ObsService.SaveReplay es bloqueante (espera la respuesta de OBS).
            var obsResult = await Task.Run(_obs.SaveReplay);

            switch (obsResult)
            {
                case SaveReplayResult.Requested:
                    result.Success = true;
                    result.Status = ReplayStatus.Saved;
                    ConsoleLog.Success($"RequestId={command.RequestId}: replay guardado correctamente.");
                    break;
                case SaveReplayResult.BufferInactive:
                    result.Status = ReplayStatus.BufferInactive;
                    result.ErrorMessage = "El Replay Buffer está apagado en OBS.";
                    ConsoleLog.Warn($"RequestId={command.RequestId}: buffer apagado, no se guardó el replay.");
                    break;
                case SaveReplayResult.NotConnected:
                    result.Status = ReplayStatus.ObsDisconnected;
                    result.ErrorMessage = "El agente no está conectado a OBS.";
                    ConsoleLog.Warn($"RequestId={command.RequestId}: OBS desconectado, no se guardó el replay.");
                    break;
                default:
                    result.Status = ReplayStatus.Error;
                    result.ErrorMessage = "OBS no pudo guardar el replay.";
                    ConsoleLog.Error($"RequestId={command.RequestId}: OBS no pudo guardar el replay.");
                    break;
            }
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.Status = ReplayStatus.Error;
            result.ErrorMessage = ex.Message;
            ConsoleLog.Error($"RequestId={command.RequestId}: error inesperado: {ex.Message}");
        }

        result.ProcessedAtUtc = DateTime.UtcNow;
        await SendResultAsync(result);
    }

    private async Task SendResultAsync(ReplayResult result)
    {
        try
        {
            await _connection.InvokeAsync(ReplayResultMethod, result, _cts?.Token ?? CancellationToken.None);
            ConsoleLog.Info($"SignalR: ReplayResult enviado (RequestId={result.RequestId}, Status={result.Status}).");
        }
        catch (Exception ex)
        {
            ConsoleLog.Error($"SignalR: no se pudo enviar ReplayResult (RequestId={result.RequestId}): {ex.Message}");
        }
    }

    private Task OnReconnecting(Exception? error)
    {
        ConsoleLog.Warn($"SignalR: conexión perdida, reconectando... {error?.GetBaseException().Message}");
        return Task.CompletedTask;
    }

    private async Task OnReconnected(string? connectionId)
    {
        ConsoleLog.Success($"SignalR: reconectado (ConnectionId {connectionId}).");
        // Nuevo ConnectionId: el servidor debe volver a asociarlo a la estación.
        await RegisterStationAsync(_cts?.Token ?? CancellationToken.None);
    }

    private Task OnClosed(Exception? error)
    {
        if (_stopping)
        {
            ConsoleLog.Info("SignalR: conexión cerrada.");
            return Task.CompletedTask;
        }

        ConsoleLog.Error($"SignalR: conexión cerrada. {error?.GetBaseException().Message}");
        // Si la reconexión automática se rindió, volvemos a empezar sin cerrar la app.
        EnsureStartLoop();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _connection.Reconnecting -= OnReconnecting;
        _connection.Reconnected -= OnReconnected;
        _connection.Closed -= OnClosed;
        await _connection.DisposeAsync();
        _cts?.Dispose();
    }

    /// <summary>
    /// Política de reintento sin límite: 0s, 2s, 5s, 10s y luego cada 30s.
    /// (La política por defecto de WithAutomaticReconnect() se rinde tras 4 intentos.)
    /// </summary>
    private sealed class ForeverRetryPolicy : IRetryPolicy
    {
        private static readonly TimeSpan[] Delays =
        {
            TimeSpan.Zero, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10)
        };

        public TimeSpan? NextRetryDelay(RetryContext retryContext) =>
            retryContext.PreviousRetryCount < Delays.Length
                ? Delays[retryContext.PreviousRetryCount]
                : TimeSpan.FromSeconds(30);
    }
}
