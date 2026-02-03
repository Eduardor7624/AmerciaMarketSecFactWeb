using AmerciaMarketSecFactWeb.Dto;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;


namespace AmerciaMarketSecFactWeb.SecEdgar
{
    public sealed class SecEdgarClient
    {
        private readonly HttpClient _http;

        private readonly Dictionary<string, int> _conceptCache = new();
        private readonly Dictionary<string, int> _unitCache = new();
        private readonly object _cacheLock = new();

        public SecEdgarClient(HttpClient http)
        {
            _http = http;
            _http.DefaultRequestHeaders.UserAgent.ParseAdd(
                "AmericaMarket/1.0 (admin@americamarket.com)"
            );
        }


        #region Get CompanyFact - o datos de la compañis
        public async Task<SecCompanyFactsDto?> GetCompanyFactsAsync(string cik)
        {
            var url = $"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json";

            const int maxRetries = 3;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

                    var response = await _http.GetAsync(url, cts.Token);

                    // ✅ OK
                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();

                        return JsonSerializer.Deserialize<SecCompanyFactsDto>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    }

                    // ⚠️ RATE LIMIT
                    if ((int)response.StatusCode == 429)
                    {
                        Debug.WriteLine($"⚠ SEC 429 for {cik} (attempt {attempt})");

                        await Task.Delay(5000); // espera fuerte
                        continue;
                    }

                    // ❌ Otros errores
                    Debug.WriteLine($"SEC failed for {cik} - {response.StatusCode}");

                    return null;
                }
                catch (TaskCanceledException)
                {
                    Debug.WriteLine($"⏱ Timeout for {cik} (attempt {attempt})");
                    await Task.Delay(2000);
                }
                catch (HttpRequestException ex)
                {
                    Debug.WriteLine($"🌐 Network error {cik}: {ex.Message}");

                    await Task.Delay(2000);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"❌ SEC exception {cik}: {ex.Message}");

                    return null;
                }
            }

            // ❌ Falló después de reintentos
            Debug.WriteLine($"❌ Failed after retries: {cik}");

            return null;
        }

        public async Task ImportCompanyFactsAsync(SecCompanyFactsDto sec, string cik, string ticker)
        {
            await using var conn = new SqlConnection(DbConnections.AmericaMarketSec);
            await conn.OpenAsync();

            int companyId = await GetOrCreateCompanyAsync(  conn, cik,  ticker,  sec.EntityName );

            foreach (var taxonomy in sec.Facts)
            {
                // taxonomy.Key = "dei" | "us-gaap"
                if (taxonomy.Value == null)
                    continue;

                foreach (var concept in taxonomy.Value)
                {
                    // concept.Key = "Assets", "Revenues", etc.
                    var conceptDto = concept.Value;
                    if (conceptDto?.Units == null)
                        continue;

                    int conceptId = await GetConceptCachedAsync(conn, taxonomy.Key, concept.Key);

                    foreach (var unit in conceptDto.Units)
                    {
                        // unit.Key = "USD", "shares"
                        if (unit.Value == null || unit.Value.Count == 0)
                            continue;

                        int unitId = await GetUnitCachedAsync(conn, unit.Key);

                        foreach (var fact in unit.Value)
                        {
                            // 🔒 Validaciones CRÍTICAS

                            // 1️⃣ valor obligatorio
                            if (fact?.Val == null)
                                continue;

                            // 2️⃣ solo filings financieros
                            if (fact.Form != "10-K" && fact.Form != "10-Q")
                                continue;

                            // 3️⃣ requiere año y periodo fiscal
                            if (fact.Fy == null || string.IsNullOrWhiteSpace(fact.Fp))
                                continue;

                            await InsertFinancialFactAsync(conn, companyId, conceptId, unitId, fact);
                        }
                    }
                }
            }
        }

        private async Task<int> GetOrCreateCompanyAsync(
            SqlConnection conn,
            string cik,
            string? ticker,
            string? name)
        {
            const string sql = @"
                                 IF NOT EXISTS (
                                     SELECT 1
                                     FROM [AmericaMarketSec].[dbo].[SecRawFactCompany]
                                     WHERE Cik = @Cik
                                 )
                                 BEGIN
                                     INSERT INTO SecRawFactCompany
                                     (
                                         Cik,
                                         Ticker,
                                         Name,
                                         CreatedDate
                                     )
                                     VALUES
                                     (
                                         @Cik,
                                         @Ticker,
                                         @Name,
                                         GETUTCDATE()
                                     )
                                 END
                                 
                                 SELECT CompanyId
                                 FROM [AmericaMarketSec].[dbo].[SecRawFactCompany]
                                 WHERE Cik = @Cik;
                                 ";

            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.Add("@Cik", SqlDbType.VarChar, 20)
                .Value = cik;

            cmd.Parameters.Add("@Ticker", SqlDbType.VarChar, 50)
                .Value = string.IsNullOrWhiteSpace(ticker)
                    ? (object)DBNull.Value
                    : ticker;

            cmd.Parameters.Add("@Name", SqlDbType.VarChar, 200)
                .Value = string.IsNullOrWhiteSpace(name)
                    ? (object)DBNull.Value
                    : name;

            var result = await cmd.ExecuteScalarAsync();

            return Convert.ToInt32(result);
        }

        private async Task<int> GetOrCreateConceptAsync(SqlConnection conn, string taxonomy, string concept)
        {
            const string sql = @"
                                 IF NOT EXISTS (
                                     SELECT 1 FROM [AmericaMarketSec].[dbo].[SecRawFactFinancialConcept] 
                                     WHERE ConceptName = @Concept AND Taxonomy = @Taxonomy
                                 )
                                 BEGIN
                                     INSERT INTO [AmericaMarketSec].[dbo].[SecRawFactFinancialConcept]
                                     (ConceptName, Taxonomy, CreatedDate)
                                     VALUES (@Concept, @Taxonomy, GETUTCDATE())
                                 END
                                 
                                 SELECT ConceptId 
                                 FROM [AmericaMarketSec].[dbo].[SecRawFactFinancialConcept]
                                 WHERE ConceptName = @Concept AND Taxonomy = @Taxonomy;
                                 ";

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Concept", concept);
            cmd.Parameters.AddWithValue("@Taxonomy", taxonomy);

            return (int)await cmd.ExecuteScalarAsync();
        }

        private async Task<int> GetOrCreateUnitAsync(SqlConnection conn, string unitCode)
        {
            const string sql = @"
                                IF NOT EXISTS (SELECT 1 FROM [AmericaMarketSec].[dbo].[SecRawFactFinancialUnit] WHERE UnitCode = @Unit)
                                BEGIN
                                    INSERT INTO [AmericaMarketSec].[dbo].[SecRawFactFinancialUnit] (UnitCode, CreatedDate)
                                    VALUES (@Unit, GETUTCDATE())
                                END
                                
                                SELECT UnitId FROM [AmericaMarketSec].[dbo].[SecRawFactFinancialUnit] WHERE UnitCode = @Unit;
                                ";

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Unit", unitCode);

            return (int)await cmd.ExecuteScalarAsync();
        }

        private async Task InsertFinancialFactAsync(
        SqlConnection conn,
        int companyId,
        int conceptId,
        int unitId,
        ConceptUnitValueDto v)
        {
            const string sql = @"
    INSERT INTO [AmericaMarketSec].[dbo].[SecRawFactFinancialFact]
    (
        CompanyId,
        ConceptId,
        UnitId,
        FiscalYear,
        FiscalPeriod,
        Value,
        FiledDate,
        FormType,
        CreatedDate
    )
    VALUES
    (
        @CompanyId,
        @ConceptId,
        @UnitId,
        @FiscalYear,
        @FiscalPeriod,
        @Value,
        @FiledDate,
        @FormType,
        GETUTCDATE()
    );";

            // ============================
            // LIMPIEZA DE DATOS
            // ============================

            decimal? value = null;

            if (v.Val != null)
            {
                if (decimal.TryParse(
                    v.Val.ToString(),
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture,
                    out var d))
                {
                    value = d;
                }
            }

            DateTime? filedDate = null;

            if (v.Filed != null)
            {
                if (DateTime.TryParse(v.Filed.ToString(), out var dt))
                {
                    filedDate = dt;
                }
            }

            using var cmd = new SqlCommand(sql, conn);

            cmd.Parameters.Add("@CompanyId", SqlDbType.Int).Value = companyId;
            cmd.Parameters.Add("@ConceptId", SqlDbType.Int).Value = conceptId;
            cmd.Parameters.Add("@UnitId", SqlDbType.Int).Value = unitId;

            cmd.Parameters.Add("@FiscalYear", SqlDbType.Int)
                .Value = v.Fy ?? (object)DBNull.Value;

            cmd.Parameters.Add("@FiscalPeriod", SqlDbType.VarChar, 10)
                .Value = v.Fp ?? (object)DBNull.Value;

            cmd.Parameters.Add("@Value", SqlDbType.Decimal)
                .Value = value ?? (object)DBNull.Value;

            cmd.Parameters.Add("@FiledDate", SqlDbType.DateTime2)
                .Value = filedDate ?? (object)DBNull.Value;

            cmd.Parameters.Add("@FormType", SqlDbType.VarChar, 10)
                .Value = v.Form ?? (object)DBNull.Value;

            try
            {
                await cmd.ExecuteNonQueryAsync();
            }
            catch (SqlException ex) when (ex.Number == 2627 || ex.Number == 2601)
            {
                // duplicado → ignorar
            }
        }

        #endregion

        public async Task<Dictionary<string, CompanyTickerItemDto>?> GetCompanyTickersAsync()
        {
            var url = "https://www.sec.gov/files/company_tickers.json";

            var response = await _http.GetAsync(url);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();

            return JsonSerializer.Deserialize<
                Dictionary<string, CompanyTickerItemDto>>(
                json,
                new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });
        }

        public async Task SaveCompanyTickersAsync(SqlConnection conn, Dictionary<string, CompanyTickerItemDto>? tickers)
        {
            if (tickers == null || tickers.Count == 0)
                return;

            const string sql = @"IF EXISTS (SELECT 1 FROM dbo.Symbols WHERE Symbol = @Symbol)
                                BEGIN
                                    UPDATE dbo.Symbols
                                    SET
                                        CentralIndexKey = @Cik,
                                        SecurityName = @Name
                                    WHERE Symbol = @Symbol;
                                END
                                ELSE
                                BEGIN
                                    INSERT INTO dbo.Symbols
                                    (
                                        Symbol,
                                        SecurityName,
                                        CentralIndexKey,
                                        Source,
                                        CreatedDate
                                    )
                                    VALUES
                                    (
                                        @Symbol,
                                        @Name,
                                        @Cik,
                                        'SEC',
                                        GETDATE()
                                    );
                                END
                                ";

            foreach (var kv in tickers)
            {
                var item = kv.Value;

                if (string.IsNullOrWhiteSpace(item.ticker))
                    continue;

                string cikPadded = item.cik_str.ToString("D10");

                using var cmd = new SqlCommand(sql, conn);

                cmd.Parameters.AddWithValue("@Symbol", item.ticker);
                cmd.Parameters.AddWithValue("@Name", item.title ?? item.ticker);
                cmd.Parameters.AddWithValue("@Cik", cikPadded);

                await cmd.ExecuteNonQueryAsync();
            }
        }

        // Get all company to update
        public async Task<List<BasicEntityDto>> GetPrincipalCompaniesAsync(bool onlyRefresh = false, int lastHours = 24)
        {
            if (onlyRefresh)
            {
                return await GetCompaniesFromRefreshSPAsync(lastHours);
            }

            return await GetCompaniesFromMainQueryAsync();
        }

        private async Task<List<BasicEntityDto>> GetCompaniesFromMainQueryAsync()
        {
            const string sql = @" SELECT DISTINCT 
                                      s.Symbol, 
                                      s.CentralIndexKey
                                  FROM dbo.Symbols s
                                  LEFT JOIN dbo.CompanyProfile cp 
                                      ON cp.Symbol = s.Symbol
                                  WHERE 
                                      s.CentralIndexKey IS NOT NULL
                                      AND s.Symbol NOT IN 
                                      (
                                          SELECT Ticker 
                                          FROM AmericaMarketSec.dbo.SecRawFactCompany
                                      )
                                  ORDER BY s.Symbol;
                              ";

            await using var conn = new SqlConnection(DbConnections.AmericaMarketCommon);

            await conn.OpenAsync();

            await using var cmd = new SqlCommand(sql, conn);

            await using var dr = await cmd.ExecuteReaderAsync();

            return await ReadCompaniesAsync(dr);
        }

        private async Task<List<BasicEntityDto>> GetCompaniesFromRefreshSPAsync(int lastHours)
        {
            await using var conn = new SqlConnection(DbConnections.AmericaMarketSec);

            await conn.OpenAsync();

            await using var cmd = new SqlCommand("dbo.sp_GetCompaniesToRefreshFacts", conn);

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Add("@LastHours", SqlDbType.Int).Value = lastHours;

            await using var dr = await cmd.ExecuteReaderAsync();

            return await ReadCompaniesAsync(dr);
        }

        private static async Task<List<BasicEntityDto>> ReadCompaniesAsync(SqlDataReader dr)
        {
            var result = new List<BasicEntityDto>();

            if (!dr.HasRows)
                return result;

            int ordSymbol = dr.GetOrdinal("Symbol");
            int ordCentralIndexKey = dr.GetOrdinal("CentralIndexKey");

            while (await dr.ReadAsync())
            {
                result.Add(new BasicEntityDto
                {
                    Id = dr.IsDBNull(ordCentralIndexKey)
                            ? null
                            : dr.GetString(ordCentralIndexKey),

                    Descrip = dr.IsDBNull(ordSymbol)
                            ? null
                            : dr.GetString(ordSymbol)
                });
            }

            return result;
        }

        private async Task<int> GetConceptCachedAsync(SqlConnection conn, string taxonomy, string concept)
        {
            var key = $"{taxonomy}:{concept}";

            lock (_cacheLock)
            {
                if (_conceptCache.TryGetValue(key, out var id))
                    return id;
            }

            var newId = await GetOrCreateConceptAsync(conn, taxonomy, concept);

            lock (_cacheLock)
            {
                _conceptCache[key] = newId;
            }

            return newId;
        }

        private async Task<int> GetUnitCachedAsync(SqlConnection conn, string unit)
        {
            lock (_cacheLock)
            {
                if (_unitCache.TryGetValue(unit, out var id))
                    return id;
            }

            var newId = await GetOrCreateUnitAsync(conn, unit);

            lock (_cacheLock)
            {
                _unitCache[unit] = newId;
            }

            return newId;
        }

    }

}
