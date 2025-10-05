using System.Data;
using System.Xml.Linq;
using System.Net.Http;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Janecek_itixo
{
    public class Program
    {
        public static string solutionPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, @"..\..\.."));
        public static string outputFile = Path.Combine(solutionPath, "data.json");
        public static string databasePath = Path.Combine(solutionPath, "database.sqlite");

        static async Task<int> Main(string[] args)
        {
            Console.WriteLine("Starting data mining.");
            Console.WriteLine("Starting data miner (type 'start', 'stop', 'quit' or 'q' to quit).");

            await EnsureSqliteDbAsync();

            var config = new ConfigurationBuilder()
                .SetBasePath(solutionPath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var url = config["DataSource:SourceUrl"];
            var intervalMin = int.TryParse(config["DataSource:PollIntervalMinutes"], out var m) ? m : 60;
            var period = TimeSpan.FromMinutes(Math.Max(1, intervalMin));

            if (string.IsNullOrWhiteSpace(url))
            {
                Console.Error.WriteLine("Missing DataSource:SourceUrl in appsettings.json");
                return 1;
            }

            url = ForcePastebinRaw(url);

            var cts = new CancellationTokenSource();
            var token = cts.Token;

            bool running = false;

            _ = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var line = Console.ReadLine();
                    if (line == null) continue;

                    line = line.Trim().ToLowerInvariant();
                    switch (line)
                    {
                        case "start":
                            running = true;
                            Console.WriteLine("Measurement loop started.");
                            break;
                        case "stop":
                            running = false;
                            Console.WriteLine("Measurement loop stopped.");
                            break;
                        case "quit" or "q":
                            Console.WriteLine("Quitting...");
                            cts.Cancel();
                            return;
                        default:
                            Console.WriteLine("Unknown command. Use: start / stop / quit / q");
                            break;
                    }
                }
            });

            while (!token.IsCancellationRequested)
            {
                if (running)
                {
                    try
                    {
                        string xmlText = await DownloadXmlAsync(url);

                        string json = ConvertXmlToJson(xmlText);

                        string finalJson = AddTimestamp(json);
                        //Console.WriteLine(finalJson);

                        await File.WriteAllTextAsync(outputFile, finalJson);
                        Console.WriteLine($"JSON saved to {outputFile}");

                        // save to DB
                        await SaveRecordAsync(url, isAvailable: true, payloadJson: finalJson, errorMessage: null);
                        Console.WriteLine("Data successfully saved to database.");
                    }
                    catch (Exception ex)
                    {
                        // empty record with unavailability flag and timestamp
                        var emptyRecord = new JObject
                        {
                            ["timestamp"] = DateTime.UtcNow.ToString("o"),
                            ["is_available"] = false,
                            ["error"] = ex.Message
                        };

                        string fallbackJson = emptyRecord.ToString(Formatting.Indented);
                        //Console.WriteLine(fallbackJson);

                        await File.WriteAllTextAsync(outputFile, fallbackJson);
                        Console.WriteLine($"JSON saved to {outputFile}");

                        // save to DB
                        await SaveRecordAsync(url, isAvailable: false, payloadJson: null, errorMessage: ex.Message);
                        Console.WriteLine("Blank data successfully saved to database.");
                    }

                    // 
                    Console.WriteLine($"Waiting {period.TotalMinutes} minutes to another reading.");
                    await Task.Delay(period);
                }
                else
                {
                    Console.WriteLine("Loop paused. Type 'start' to resume.");
                }

                await Task.Delay(period, token).ContinueWith(_ => { });
            }
            return 0;
        }

        private static string ForcePastebinRaw(string url)
        {
            // https://pastebin.com/PMQueqDV  -> https://pastebin.com/raw/PMQueqDV
            if (url.Contains("pastebin.com", StringComparison.OrdinalIgnoreCase) && !url.Contains("/raw/", StringComparison.OrdinalIgnoreCase))
            {
                var parts = url.TrimEnd('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
                var last = parts.Last(); // ID
                return $"https://pastebin.com/raw/{last}";
            }
            return url;
        }

        private static async Task<string> DownloadXmlAsync(string url)
        {
            using var http = new HttpClient(new HttpClientHandler
            {
                AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate
            });
            http.Timeout = TimeSpan.FromSeconds(30);
            http.DefaultRequestHeaders.UserAgent.ParseAdd("MeteoIngestor/1.0");

            var resp = await http.GetAsync(url);
            if (!resp.IsSuccessStatusCode)
            {
                throw new InvalidOperationException($"HTTP {(int)resp.StatusCode} {resp.ReasonPhrase}");
            }    

            var xmlText = await resp.Content.ReadAsStringAsync();
            if (string.IsNullOrWhiteSpace(xmlText))
            {
                throw new InvalidOperationException("Empty answer");
            }
                

            try { 
                _ = XDocument.Parse(xmlText, LoadOptions.PreserveWhitespace); 
            }
            catch (Exception ex) 
            { 
                throw new InvalidOperationException($"Nonvalid XML: {ex.Message}"); 
            }

            return xmlText;
        }

        private static string ConvertXmlToJson(string xmlText)
        {
            var xdoc = XDocument.Parse(xmlText, LoadOptions.PreserveWhitespace);

            var json = JsonConvert.SerializeXNode(xdoc, Formatting.None, omitRootObject: false);

            _ = JObject.Parse(json);

            return json;
        }

        private static string AddTimestamp(string json)
        {
            var obj = JObject.Parse(json);
            obj["timestamp"] = DateTime.UtcNow.ToString("o");
            return obj.ToString(Formatting.Indented); 
        }

        // SQL database 
        static string GetSqliteConnectionString()
        {
            return new SqliteConnectionStringBuilder
            {
                DataSource = databasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Default
            }.ToString();
        }

        static async Task EnsureSqliteDbAsync()
        {
            Directory.CreateDirectory(solutionPath);

            if (File.Exists(databasePath))
            {
                try
                {
                    await using var testConn = new SqliteConnection(GetSqliteConnectionString());
                    await testConn.OpenAsync();

                    await using var pragma = testConn.CreateCommand();
                    pragma.CommandText = "PRAGMA schema_version;";
                    await pragma.ExecuteScalarAsync();
                }
                catch (SqliteException)
                {
                    try 
                    { 
                        File.Delete(databasePath); 
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"Can't delete nonvalid DB´file: {ex.Message}");
                        throw;
                    }
                }
            }

            await using var conn = new SqliteConnection(GetSqliteConnectionString());
            await conn.OpenAsync();

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS WeatherReadings (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Timestamp TEXT NOT NULL,
                    SourceUrl TEXT NOT NULL,
                    IsAvailable INTEGER NOT NULL,
                    PayloadJson TEXT NULL,
                    ErrorMessage TEXT NULL
                );";
                await cmd.ExecuteNonQueryAsync();

                // CREATE INDEX 
                cmd.CommandText = @"
                CREATE INDEX IF NOT EXISTS IX_WeatherReadings_Timestamp ON WeatherReadings(Timestamp DESC);";
                await cmd.ExecuteNonQueryAsync();
            }
        }

        static async Task SaveRecordAsync(string sourceUrl, bool isAvailable, string? payloadJson, string? errorMessage)
        {
            await using var conn = new SqliteConnection(GetSqliteConnectionString());
            await conn.OpenAsync();

            var cmd = conn.CreateCommand();
            cmd.CommandText = @"
            INSERT INTO WeatherReadings (Timestamp, SourceUrl, IsAvailable, PayloadJson, ErrorMessage)
            VALUES ($ts, $url, $avail, $payload, $err);";
            cmd.Parameters.AddWithValue("$ts", DateTime.UtcNow.ToString("o"));
            cmd.Parameters.AddWithValue("$url", sourceUrl);
            cmd.Parameters.AddWithValue("$avail", isAvailable ? 1 : 0);
            cmd.Parameters.AddWithValue("$payload", (object?)payloadJson ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$err", (object?)errorMessage ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync();
        }


    }
}
