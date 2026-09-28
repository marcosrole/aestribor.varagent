namespace Aestribor.VarAgent;

/// <summary>
/// Escritura a consola thread-safe (los eventos de OBS llegan desde otros hilos).
/// </summary>
public static class ConsoleLog
{
    private static readonly object Sync = new();

    public static void Info(string message) => Write(message, ConsoleColor.Gray);
    public static void Success(string message) => Write(message, ConsoleColor.Green);
    public static void Warn(string message) => Write(message, ConsoleColor.Yellow);
    public static void Error(string message) => Write(message, ConsoleColor.Red);

    private static void Write(string message, ConsoleColor color)
    {
        lock (Sync)
        {
            var previous = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {message}");
            Console.ForegroundColor = previous;
        }
    }
}
