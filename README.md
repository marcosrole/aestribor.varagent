# Aestribor.VarAgent

Agente local (.NET 8, consola, Windows) que corre en cada estación VAR. Se conecta a OBS Studio por obs-websocket v5 y al Hub SignalR de Aestribor, y guarda el Replay Buffer cuando recibe el comando remoto `SaveReplay` (o al presionar ENTER).

## Configuración

`appsettings.json` no se versiona porque contiene credenciales. Copiá la plantilla y completala:

```
copy appsettings.example.json appsettings.json
```

- `Obs:Password`: contraseña de obs-websocket (OBS → Herramientas → Configuración del servidor WebSocket).
- `Aestribor:StationId`: identificador único de esta estación.
- `Aestribor:ApiKey`: se envía en el header `X-Api-Key` si no está vacío.

## Ejecutar

```
dotnet run
```

ENTER = guardar replay · Q = salir.

## Contrato con el Hub (`/hubs/var`)

| Dirección | Método | Payload |
|---|---|---|
| Agente → Hub | `RegisterStation` | `string stationId` (al conectar y al reconectar) |
| Hub → Agente | `SaveReplay` | `SaveReplayCommand` |
| Agente → Hub | `ReplayResult` | `ReplayResult` (`Status`: `SAVED`, `BUFFER_INACTIVE`, `OBS_DISCONNECTED`, `ERROR`) |
