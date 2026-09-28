using Aestribor.VarAgent;
using Microsoft.Extensions.Configuration;

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.Title = "Aestribor.VarAgent";

IConfiguration config;
try
{
    config = new ConfigurationBuilder()
        .SetBasePath(AppContext.BaseDirectory)
        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
        .Build();
}
catch (Exception ex)
{
    ConsoleLog.Error($"No se pudo leer appsettings.json: {ex.Message}");
    return 1;
}

var aestriborSettings = config.GetSection("Aestribor").Get<AestriborSettings>() ?? new AestriborSettings();
var obsSettings = config.GetSection("Obs").Get<ObsSettings>() ?? new ObsSettings();

if (!obsSettings.Url.StartsWith("ws://", StringComparison.OrdinalIgnoreCase) &&
    !obsSettings.Url.StartsWith("wss://", StringComparison.OrdinalIgnoreCase))
{
    ConsoleLog.Error($"Obs:Url inválida '{obsSettings.Url}'. Debe empezar con ws:// (ej. ws://127.0.0.1:4455).");
    return 1;
}

if (!Uri.TryCreate(aestriborSettings.SignalRHubUrl, UriKind.Absolute, out var hubUri) ||
    (hubUri.Scheme != Uri.UriSchemeHttp && hubUri.Scheme != Uri.UriSchemeHttps))
{
    ConsoleLog.Error($"Aestribor:SignalRHubUrl inválida '{aestriborSettings.SignalRHubUrl}'. Debe ser http:// o https://.");
    return 1;
}

if (string.IsNullOrWhiteSpace(aestriborSettings.StationId))
{
    ConsoleLog.Error("Aestribor:StationId es obligatorio en appsettings.json.");
    return 1;
}

Console.WriteLine("==============================================");
Console.WriteLine($" Aestribor.VarAgent - Estación: {aestriborSettings.StationId}");
Console.WriteLine("  ENTER = guardar replay   |   Q = salir");
Console.WriteLine("==============================================");

using var obs = new ObsService(obsSettings);
obs.Start();

using var exitCts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // Ctrl+C cierra ordenadamente, igual que Q
    exitCts.Cancel();
};

await using var signalR = new SignalRService(aestriborSettings, obs);
signalR.Start(exitCts.Token);

while (!exitCts.IsCancellationRequested)
{
    if (!Console.KeyAvailable)
    {
        try { await Task.Delay(50, exitCts.Token); } catch (OperationCanceledException) { }
        continue;
    }

    var key = Console.ReadKey(intercept: true).Key;

    if (key == ConsoleKey.Q)
    {
        break;
    }

    if (key == ConsoleKey.Enter)
    {
        // SaveReplay es bloqueante (espera respuesta de OBS); se corre fuera del hilo de consola.
        var result = await Task.Run(obs.SaveReplay);

        switch (result)
        {
            case SaveReplayResult.Requested:
                ConsoleLog.Info("Guardando replay...");
                break;
            case SaveReplayResult.NotConnected:
                ConsoleLog.Warn("No hay conexión con OBS. No se guardó el replay.");
                break;
            case SaveReplayResult.BufferInactive:
                ConsoleLog.Warn("El Replay Buffer está INACTIVO. No se guardó el replay.");
                break;
            case SaveReplayResult.Failed:
                ConsoleLog.Error("No se pudo guardar el replay.");
                break;
        }
    }
}

ConsoleLog.Info("Cerrando...");
await signalR.StopAsync();
await obs.StopAsync();
return 0;
