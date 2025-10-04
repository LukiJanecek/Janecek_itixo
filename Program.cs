using System.Data;
using System.Xml.Linq;
using System.Net.Http;
using Dapper;
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
        static async Task<int> Main(string[] args)
        {
            Console.WriteLine("Starting data mining.");

            string solutionPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, @"..\..\.."));
            string outputFile = Path.Combine(solutionPath, "data.json");

            var config = new ConfigurationBuilder()
                .SetBasePath(solutionPath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var url = config["DataSource:SourceUrl"];

            if (string.IsNullOrWhiteSpace(url))
            {
                Console.Error.WriteLine("Missing DataSource:SourceUrl in appsettings.json");
                return 1;
            }

            url = ForcePastebinRaw(url);

            try
            {
                string xmlText = await DownloadXmlAsync(url);

                string json = ConvertXmlToJson(xmlText);

                string finalJson = AddTimestamp(json);
                //Console.WriteLine(finalJson);

                await File.WriteAllTextAsync(outputFile, finalJson);
                Console.WriteLine($"JSON uložen do {outputFile}");

                return 0;
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
                Console.WriteLine($"JSON uložen do {outputFile}");

                return 2;
            }
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
                throw new InvalidOperationException("Prázdná odpověď");
            }
                

            try { 
                _ = XDocument.Parse(xmlText, LoadOptions.PreserveWhitespace); 
            }
            catch (Exception ex) 
            { 
                throw new InvalidOperationException($"Nevalidní XML: {ex.Message}"); 
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
    }
}
