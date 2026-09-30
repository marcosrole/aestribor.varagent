using OBSWebsocketDotNet;
using OBSWebsocketDotNet.Communication;
using OBSWebsocketDotNet.Types;
using OBSWebsocketDotNet.Types.Events;

namespace Aestribor.VarAgent;

public enum SaveReplayResult
{
    Requested,
    NotConnected,
    BufferInactive,
    Failed
}

public enum CaptureOutcome
{
    Changed,
    NoChange,
    NotConnected,
    Failed
}

/// <param name="RecordingPath">Archivo de grabación generado al detener (solo en StopCapture).</param>
public sealed record CaptureOperationResult(CaptureOutcome Outcome, string? ErrorMessage = null, string? RecordingPath = null);

/// <summary>
/// Encapsula la conexión a OBS (obs-websocket v5) y las operaciones del Replay Buffer.
/// Mantiene un bucle en segundo plano que reconecta si OBS se cierra o se pierde la conexión.
/// </summary>
public sealed class ObsService : IDisposable
{
    private readonly ObsSettings _settings;
    private readonly OBSWebsocket _obs = new();

    // Resultado del intento de conexión en curso: null = conectado, ObsDisconnectionInfo = falló.
    private TaskCompletionSource<ObsDisconnectionInfo?>? _pendingConnect;
    private int _online; // 1 cuando ya se anunció "conectado"
    private volatile bool _stopping;
    private CancellationTokenSource? _loopCts;
    private Task? _loopTask;

    public ObsService(ObsSettings settings)
    {
        _settings = settings;

        _obs.Connected += OnConnected;
        _obs.Disconnected += OnDisconnected;
        _obs.ReplayBufferStateChanged += OnReplayBufferStateChanged;
        _obs.ReplayBufferSaved += OnReplayBufferSaved;
    }

    public bool IsConnected => _obs.IsConnected;

    /// <summary>Inicia el bucle de conexión/reconexión en segundo plano.</summary>
    public void Start()
    {
        _loopCts = new CancellationTokenSource();
        _loopTask = Task.Run(() => ConnectionLoopAsync(_loopCts.Token));
    }

    public async Task StopAsync()
    {
        _stopping = true;
        _loopCts?.Cancel();

        if (_loopTask != null)
        {
            try { await _loopTask; } catch (OperationCanceledException) { }
        }

        if (_obs.IsConnected)
        {
            _obs.Disconnect();
        }
    }

    /// <summary>
    /// Consulta el estado del Replay Buffer. Devuelve null si no se pudo consultar.
    /// </summary>
    public bool? GetReplayBufferActive()
    {
        if (!_obs.IsConnected)
        {
            return null;
        }

        try
        {
            return _obs.GetReplayBufferStatus();
        }
        catch (ErrorResponseException ex)
        {
            ConsoleLog.Error($"OBS rechazó GetReplayBufferStatus (código {ex.ErrorCode}): {ex.Message}");
        }
        catch (Exception ex)
        {
            ConsoleLog.Error($"No se pudo consultar el Replay Buffer: {ex.Message}");
        }

        return null;
    }

    /// <summary>
    /// Pide a OBS que guarde el Replay Buffer. Solo lo intenta si el buffer está activo.
    /// La confirmación con la ruta del archivo llega por el evento ReplayBufferSaved.
    /// </summary>
    public SaveReplayResult SaveReplay()
    {
        if (!_obs.IsConnected)
        {
            return SaveReplayResult.NotConnected;
        }

        var active = GetReplayBufferActive();
        if (active is null)
        {
            return SaveReplayResult.Failed;
        }

        if (active == false)
        {
            return SaveReplayResult.BufferInactive;
        }

        try
        {
            _obs.SaveReplayBuffer();
            return SaveReplayResult.Requested;
        }
        catch (ErrorResponseException ex)
        {
            ConsoleLog.Error($"OBS rechazó SaveReplayBuffer (código {ex.ErrorCode}): {ex.Message}");
        }
        catch (Exception ex)
        {
            ConsoleLog.Error($"Error al guardar el replay: {ex.Message}");
        }

        return SaveReplayResult.Failed;
    }

    /// <summary>
    /// Inicia la grabación y el Replay Buffer, solo lo que no esté ya activo
    /// (puede haber otro heat en curso que ya los inició).
    /// </summary>
    public CaptureOperationResult StartCapture()
    {
        if (!_obs.IsConnected)
        {
            return new CaptureOperationResult(CaptureOutcome.NotConnected);
        }

        try
        {
            bool changed = false;

            if (!_obs.GetRecordStatus().IsRecording)
            {
                _obs.StartRecord();
                ConsoleLog.Success("Grabación iniciada.");
                changed = true;
            }

            if (!_obs.GetReplayBufferStatus())
            {
                _obs.StartReplayBuffer();
                changed = true; // el evento ReplayBufferStateChanged informa "Replay Buffer ACTIVO"
            }

            return new CaptureOperationResult(changed ? CaptureOutcome.Changed : CaptureOutcome.NoChange);
        }
        catch (ErrorResponseException ex)
        {
            ConsoleLog.Error($"OBS rechazó el inicio de la captura (código {ex.ErrorCode}): {ex.Message}");
            return new CaptureOperationResult(CaptureOutcome.Failed, ex.Message);
        }
        catch (Exception ex)
        {
            ConsoleLog.Error($"Error al iniciar la captura: {ex.Message}");
            return new CaptureOperationResult(CaptureOutcome.Failed, ex.Message);
        }
    }

    /// <summary>Detiene la grabación y el Replay Buffer, solo lo que esté activo.</summary>
    public CaptureOperationResult StopCapture()
    {
        if (!_obs.IsConnected)
        {
            return new CaptureOperationResult(CaptureOutcome.NotConnected);
        }

        try
        {
            bool changed = false;
            string? recordingPath = null;

            if (_obs.GetRecordStatus().IsRecording)
            {
                recordingPath = _obs.StopRecord();
                ConsoleLog.Success($"Grabación detenida: {recordingPath}");
                changed = true;
            }

            if (_obs.GetReplayBufferStatus())
            {
                _obs.StopReplayBuffer();
                changed = true; // el evento ReplayBufferStateChanged informa "Replay Buffer INACTIVO"
            }

            return new CaptureOperationResult(changed ? CaptureOutcome.Changed : CaptureOutcome.NoChange, RecordingPath: recordingPath);
        }
        catch (ErrorResponseException ex)
        {
            ConsoleLog.Error($"OBS rechazó la detención de la captura (código {ex.ErrorCode}): {ex.Message}");
            return new CaptureOperationResult(CaptureOutcome.Failed, ex.Message);
        }
        catch (Exception ex)
        {
            ConsoleLog.Error($"Error al detener la captura: {ex.Message}");
            return new CaptureOperationResult(CaptureOutcome.Failed, ex.Message);
        }
    }

    private async Task ConnectionLoopAsync(CancellationToken ct)
    {
        var reconnectDelay = TimeSpan.FromSeconds(Math.Max(1, _settings.ReconnectDelaySeconds));

        while (!ct.IsCancellationRequested)
        {
            if (!_obs.IsConnected)
            {
                bool connected = await TryConnectAsync(ct);
                if (connected)
                {
                    ReportReplayBufferStatus();
                }
                else
                {
                    ConsoleLog.Warn($"Reintentando en {reconnectDelay.TotalSeconds:0} s...");
                    await Task.Delay(reconnectDelay, ct);
                    continue;
                }
            }

            // Watchdog: si la conexión se cae, IsConnected pasa a false y se reintenta.
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
    }

    private async Task<bool> TryConnectAsync(CancellationToken ct)
    {
        ConsoleLog.Info($"Conectando a OBS en {_settings.Url} ...");

        var tcs = new TaskCompletionSource<ObsDisconnectionInfo?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingConnect = tcs;

        try
        {
            // En obs-websocket-dotnet 5.x ConnectAsync devuelve void: el resultado
            // llega por los eventos Connected / Disconnected.
            _obs.ConnectAsync(_settings.Url, _settings.Password);
        }
        catch (Exception ex)
        {
            _pendingConnect = null;
            ConsoleLog.Error($"Error de conexión: {ex.Message}");
            return false;
        }

        var timeout = TimeSpan.FromSeconds(Math.Max(1, _settings.ConnectTimeoutSeconds));
        var finished = await Task.WhenAny(tcs.Task, Task.Delay(timeout, ct));
        _pendingConnect = null;

        if (finished != tcs.Task)
        {
            ct.ThrowIfCancellationRequested();
            ConsoleLog.Error("Error de conexión: OBS no respondió a tiempo (¿OBS está abierto y con WebSocket habilitado?).");
            SafeDisconnect();
            return false;
        }

        var failure = await tcs.Task;
        if (failure is null)
        {
            return true;
        }

        if (failure.ObsCloseCode == ObsCloseCodes.AuthenticationFailed)
        {
            ConsoleLog.Error("Error de autenticación: la contraseña de obs-websocket es incorrecta. Revisá appsettings.json.");
        }
        else
        {
            ConsoleLog.Error($"Error de conexión: {DescribeDisconnection(failure)}");
        }

        return false;
    }

    private void ReportReplayBufferStatus()
    {
        var active = GetReplayBufferActive();
        if (active == true)
        {
            ConsoleLog.Success("Replay Buffer ACTIVO.");
        }
        else if (active == false)
        {
            ConsoleLog.Warn("Replay Buffer INACTIVO. Inicialo en OBS para poder guardar replays.");
        }
    }

    private void OnConnected(object? sender, EventArgs e)
    {
        Interlocked.Exchange(ref _online, 1);
        ConsoleLog.Success("Conectado a OBS.");
        _pendingConnect?.TrySetResult(null);
    }

    private void OnDisconnected(object? sender, ObsDisconnectionInfo info)
    {
        bool wasOnline = Interlocked.Exchange(ref _online, 0) == 1;

        var pending = _pendingConnect;
        if (pending != null && !wasOnline)
        {
            // Falló el intento de conexión en curso; TryConnectAsync informa el motivo.
            pending.TrySetResult(info);
            return;
        }

        if (wasOnline && !_stopping)
        {
            ConsoleLog.Error($"Se perdió la conexión con OBS: {DescribeDisconnection(info)}");
        }
    }

    private void OnReplayBufferStateChanged(object? sender, ReplayBufferStateChangedEventArgs e)
    {
        switch (e.OutputState.State)
        {
            case OutputState.OBS_WEBSOCKET_OUTPUT_STARTED:
                ConsoleLog.Success("Replay Buffer ACTIVO.");
                break;
            case OutputState.OBS_WEBSOCKET_OUTPUT_STOPPED:
                ConsoleLog.Warn("Replay Buffer INACTIVO.");
                break;
        }
    }

    private void OnReplayBufferSaved(object? sender, ReplayBufferSavedEventArgs e)
    {
        ConsoleLog.Success($"Replay guardado: {e.SavedReplayPath}");
    }

    private void SafeDisconnect()
    {
        try { _obs.Disconnect(); } catch { /* ignorar: ya está desconectado */ }
    }

    private static string DescribeDisconnection(ObsDisconnectionInfo info)
    {
        var wsException = info.WebsocketDisconnectionInfo?.Exception;
        if (wsException != null)
        {
            return wsException.GetBaseException().Message;
        }

        if (!string.IsNullOrWhiteSpace(info.DisconnectReason))
        {
            return $"{info.DisconnectReason} ({info.ObsCloseCode})";
        }

        return info.WebsocketDisconnectionInfo?.Type.ToString() ?? info.ObsCloseCode.ToString();
    }

    public void Dispose()
    {
        _obs.Connected -= OnConnected;
        _obs.Disconnected -= OnDisconnected;
        _obs.ReplayBufferStateChanged -= OnReplayBufferStateChanged;
        _obs.ReplayBufferSaved -= OnReplayBufferSaved;
        _loopCts?.Dispose();
    }
}
