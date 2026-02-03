using AmerciaMarketSecFactWeb.Dto;
using AmerciaMarketSecFactWeb.SecEdgar;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using static System.Net.WebRequestMethods;


namespace AmerciaMarketSecFactWeb
{
    class Program
    {
        private static int _secCurrentCount = 0;
        private static HttpClient _httpClientSEC;

        static async Task Main()
        {
            Console.WriteLine("=== SEC CompanyFacts Importer ===");

            _httpClientSEC = BuildSecClient();

            var program = new Program();

            try
            {
                await program.RunSecEdgarJobAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("[FATAL]");
                Console.WriteLine(ex);
            }

            Console.WriteLine("Finished.");
            Console.ReadLine();
        }

        // ===============================
        // MAIN JOB
        // ===============================
        private async Task RunSecEdgarJobAsync()
        {
            // variables for log 
            string? requestUrl = null;
            string? responseText = null;
            DateTime? sentAt = null;
            DateTime? receivedAt = null;
            string? exceptionText = null;

            var client = new SecEdgarClient(_httpClientSEC);

            // Get companies to update
            var companiesList = await GetCompaniesFromRefreshSPAsync(); 

            Console.WriteLine($"Companies: {companiesList.Count}");

            int total = companiesList.Count;

            _secCurrentCount = 0;

            foreach (var company in companiesList)
            {
                _secCurrentCount++;

                Console.WriteLine($"➡ {_secCurrentCount}/{total} {company.Descrip}");

                try
                {
                    sentAt = DateTime.UtcNow;
                    bool ok = await ImportCompanyFacts( company.Id, company.Descrip, client );

                    if (ok)
                    {
                        Console.WriteLine($"✔ OK: {company.Descrip}");
                    }
                    else
                    {
                        Console.WriteLine($"⚠ SKIP: {company.Descrip}");
                    }
                }
                catch (Exception ex)
                {
                    receivedAt = DateTime.UtcNow;
                    Console.WriteLine($"❌ ERROR: {company.Descrip}");
                    Console.WriteLine(ex); 
                    await SecLog.LogAsync($"https://data.sec.gov/api/xbrl/companyfacts/CIK{company.Id}.json", "[ERROR]", sentAt, receivedAt, $"[ERR]  {company.Id.ToString()} " + " - " + ex.Message, "SecFact");
                }

                // Respeta rate limit SEC
                await Task.Delay(3000);
            }

            Console.WriteLine("✅ SEC import completed");
        }
         

        // ===============================
        // IMPORT
        // ===============================
        private async Task<bool> ImportCompanyFacts( string cik,  string name,  SecEdgarClient client)
        {
            var sec = await client.GetCompanyFactsAsync(cik);

            if (sec == null)
            {
                Console.WriteLine($"[SKIP] {name} ({cik})");
                return false;
            }

            await client.ImportCompanyFactsAsync(sec, cik, name);

            return true;
        }

        // ===============================
        // HTTP CLIENT
        // ===============================
        private static HttpClient BuildSecClient()
        {
            var http = new HttpClient();

            http.DefaultRequestHeaders.UserAgent.ParseAdd(
                "AmericaMarket/1.0 (admin@americamarket.com)"
            );

            http.DefaultRequestHeaders.Add(
                "From",
                "admin@americamarket.com"
            );

            http.Timeout = TimeSpan.FromSeconds(60);

            return http;
        }


        // ===============================
        // DB get list of Companies to Update from  sp_GetCompaniesToRefreshFacts 
        // ===============================
        public async Task<List<BasicEntityDto>> GetCompaniesFromRefreshSPAsync()
        {
            await using var conn =
                new SqlConnection(DbConnections.AmericaMarketSec);

            await conn.OpenAsync();

            await using var cmd =
                new SqlCommand("dbo.sp_GetCompaniesToRefreshFacts", conn);

            cmd.CommandType = CommandType.StoredProcedure;

            await using var dr =
                await cmd.ExecuteReaderAsync();

            return await ReadCompaniesAsync(dr);
        }

        public static async Task<List<BasicEntityDto>> ReadCompaniesAsync(SqlDataReader dr)
        {
            var result = new List<BasicEntityDto>();

            if (!dr.HasRows)
                return result;

            int ordSymbol = dr.GetOrdinal("Symbol");
            int ordCik = dr.GetOrdinal("CentralIndexKey");

            while (await dr.ReadAsync())
            {
                result.Add(new BasicEntityDto
                {
                    Id = dr.IsDBNull(ordCik)
                            ? null
                            : dr.GetString(ordCik),

                    Descrip = dr.IsDBNull(ordSymbol)
                            ? null
                            : dr.GetString(ordSymbol)
                });
            }

            return result;
        }
    }
}