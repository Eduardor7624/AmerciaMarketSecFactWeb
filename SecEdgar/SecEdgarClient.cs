using AmerciaMarketSecFactWeb.Dto;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AmerciaMarketSecFactWeb.SecEdgar
{
    public sealed class SecEdgarClient
    {
        private readonly HttpClient _http;

        // ============================
        // CACHE EN MEMORIA
        // ============================

        private readonly Dictionary<string, int> _conceptCache = new();
        private readonly Dictionary<string, int> _unitCache = new();

        private readonly object _lock = new();

        public SecEdgarClient(HttpClient http)
        {
            _http = http;
        }

        // ============================
        // GET COMPANY FACTS
        // ============================

        public async Task<SecCompanyFactsDto?> GetCompanyFactsAsync(string cik)
        {
            var url = $"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json";

            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

                var res = await _http.GetAsync(url, cts.Token);

                if (!res.IsSuccessStatusCode)
                    return null;

                var json = await res.Content.ReadAsStringAsync();

                return JsonSerializer.Deserialize<SecCompanyFactsDto>(
                    json,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });
            }
            catch
            {
                return null;
            }
        }

        // ============================
        // MAIN IMPORT
        // ============================

        public async Task ImportCompanyFactsAsync(SecCompanyFactsDto sec, string cik, string ticker)
        {
            await using var conn =
                new SqlConnection(DbConnections.AmericaMarketSec);

            await conn.OpenAsync();

            // 🔥 TRANSACCION POR COMPAÑIA
            using var tx = conn.BeginTransaction();

            try
            {
                int companyId =
                    await GetOrCreateCompanyAsync(conn, tx, cik, ticker, sec.EntityName);

                foreach (var taxonomy in sec.Facts)
                {
                    if (taxonomy.Value == null)
                        continue;

                    foreach (var concept in taxonomy.Value)
                    {
                        var conceptDto = concept.Value;

                        if (conceptDto?.Units == null)
                            continue;

                        int conceptId =
                            await GetConceptCachedAsync(
                                conn, tx,
                                taxonomy.Key,
                                concept.Key);

                        foreach (var unit in conceptDto.Units)
                        {
                            if (unit.Value == null || unit.Value.Count == 0)
                                continue;

                            int unitId =
                                await GetUnitCachedAsync(
                                    conn, tx,
                                    unit.Key);

                            foreach (var fact in unit.Value)
                            {
                                if (fact?.Val == null)
                                    continue;

                                if (fact.Form != "10-K" &&
                                    fact.Form != "10-Q")
                                    continue;

                                if (fact.Fy == null ||
                                    string.IsNullOrWhiteSpace(fact.Fp))
                                    continue;

                                await UpsertFinancialFactAsync(
                                    conn,
                                    tx,
                                    companyId,
                                    conceptId,
                                    unitId,
                                    fact);
                            }
                        }
                    }
                }

                // ✅ COMMIT
                await tx.CommitAsync();
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }
        }

        // ============================
        // COMPANY
        // ============================

        private async Task<int> GetOrCreateCompanyAsync( SqlConnection conn, SqlTransaction tx, string cik, string? ticker, string? name)
        {
            const string sql = @"UPDATE SecRawFactCompany
                                 SET
                                  Ticker=@Ticker,
                                  Name=@Name,
                                  UpdatedDate=SYSUTCDATETIME()
                                 WHERE Cik=@Cik;
                                 
                                 IF @@ROWCOUNT=0
                                 BEGIN
                                  INSERT INTO SecRawFactCompany
                                  (
                                   Cik,Ticker,Name,CreatedDate,UpdatedDate
                                  )
                                  VALUES
                                  (
                                   @Cik,@Ticker,@Name,
                                   SYSUTCDATETIME(),SYSUTCDATETIME()
                                  );
                                 END
                                 
                                 SELECT CompanyId
                                 FROM SecRawFactCompany
                                 WHERE Cik=@Cik;
                                 ";

            using var cmd =
                new SqlCommand(sql, conn, tx);

            cmd.Parameters.Add("@Cik", SqlDbType.VarChar, 20).Value = cik;

            cmd.Parameters.Add("@Ticker", SqlDbType.VarChar, 50)
                .Value = string.IsNullOrWhiteSpace(ticker)
                ? DBNull.Value
                : ticker;

            cmd.Parameters.Add("@Name", SqlDbType.VarChar, 200)
                .Value = string.IsNullOrWhiteSpace(name)
                ? DBNull.Value
                : name;

            return (int)await cmd.ExecuteScalarAsync();
        }

        // ============================
        // CONCEPT (CACHE)
        // ============================

        private async Task<int> GetConceptCachedAsync(
            SqlConnection conn,
            SqlTransaction tx,
            string taxonomy,
            string concept)
        {
            var key = $"{taxonomy}:{concept}";

            lock (_lock)
            {
                if (_conceptCache.TryGetValue(key, out var id))
                    return id;
            }

            int newId =
                await GetOrCreateConceptAsync(
                    conn, tx,
                    taxonomy,
                    concept);

            lock (_lock)
            {
                _conceptCache[key] = newId;
            }

            return newId;
        }

        private async Task<int> GetOrCreateConceptAsync(
            SqlConnection conn,
            SqlTransaction tx,
            string taxonomy,
            string concept)
        {
            const string sql = @"IF NOT EXISTS
                                (
                                 SELECT 1
                                 FROM SecRawFactFinancialConcept
                                 WHERE ConceptName=@Concept
                                 AND Taxonomy=@Taxonomy
                                )
                                BEGIN
                                 INSERT INTO SecRawFactFinancialConcept
                                 (
                                  ConceptName,Taxonomy,CreatedDate
                                 )
                                 VALUES
                                 (
                                  @Concept,@Taxonomy,SYSUTCDATETIME()
                                 );
                                END
                                
                                SELECT ConceptId
                                FROM SecRawFactFinancialConcept
                                WHERE ConceptName=@Concept
                                AND Taxonomy=@Taxonomy;
                                ";

            using var cmd =
                new SqlCommand(sql, conn, tx);

            cmd.Parameters.AddWithValue("@Concept", concept);
            cmd.Parameters.AddWithValue("@Taxonomy", taxonomy);

            return (int)await cmd.ExecuteScalarAsync();
        }

        // ============================
        // UNIT (CACHE)
        // ============================

        private async Task<int> GetUnitCachedAsync(
            SqlConnection conn,
            SqlTransaction tx,
            string unit)
        {
            lock (_lock)
            {
                if (_unitCache.TryGetValue(unit, out var id))
                    return id;
            }

            int newId =
                await GetOrCreateUnitAsync(conn, tx, unit);

            lock (_lock)
            {
                _unitCache[unit] = newId;
            }

            return newId;
        }

        private async Task<int> GetOrCreateUnitAsync(
            SqlConnection conn,
            SqlTransaction tx,
            string unit)
        {
            const string sql = @"IF NOT EXISTS
                                (
                                 SELECT 1
                                 FROM SecRawFactFinancialUnit
                                 WHERE UnitCode=@Unit
                                )
                                BEGIN
                                 INSERT INTO SecRawFactFinancialUnit
                                 (
                                  UnitCode,CreatedDate
                                 )
                                 VALUES
                                 (
                                  @Unit,SYSUTCDATETIME()
                                 );
                                END
                                
                                SELECT UnitId
                                FROM SecRawFactFinancialUnit
                                WHERE UnitCode=@Unit;
                                ";

            using var cmd =
                new SqlCommand(sql, conn, tx);

            cmd.Parameters.AddWithValue("@Unit", unit);

            return (int)await cmd.ExecuteScalarAsync();
        }

        // ============================
        // FACT UPSERT (FAST)
        // ============================

        private async Task UpsertFinancialFactAsync( SqlConnection conn, SqlTransaction tx, int companyId, int conceptId, int unitId, ConceptUnitValueDto v)
        {
            const string sql = @"UPDATE SecRawFactFinancialFact
                               SET
                                Value=@Value,
                                FiledDate=@FiledDate,
                                AccessionNumber=@Accession,
                                UpdatedDate=SYSUTCDATETIME()
                               WHERE
                                CompanyId=@CompanyId
                                AND ConceptId=@ConceptId
                                AND UnitId=@UnitId
                                AND FiscalYear=@FiscalYear
                                AND FiscalPeriod=@FiscalPeriod
                                AND FormType=@FormType;
                               
                               IF @@ROWCOUNT=0
                               BEGIN
                                INSERT INTO SecRawFactFinancialFact
                                (
                                 CompanyId,ConceptId,UnitId,
                                 FiscalYear,FiscalPeriod,FormType,
                                 AccessionNumber,Value,FiledDate,
                                 CreatedDate,UpdatedDate
                                )
                                VALUES
                                (
                                 @CompanyId,@ConceptId,@UnitId,
                                 @FiscalYear,@FiscalPeriod,@FormType,
                                 @Accession,@Value,@FiledDate,
                                 SYSUTCDATETIME(),SYSUTCDATETIME()
                                );
                               END
                               ";

            decimal? value = null;

            if (v.Val != null &&
                decimal.TryParse(
                    v.Val.ToString(),
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture,
                    out var d))
            {
                value = d;
            }

            DateTime? filed = null;

            if (DateTime.TryParse(v.Filed?.ToString(), out var dt))
                filed = dt;

            using var cmd =
                new SqlCommand(sql, conn, tx);

            cmd.Parameters.AddWithValue("@CompanyId", companyId);
            cmd.Parameters.AddWithValue("@ConceptId", conceptId);
            cmd.Parameters.AddWithValue("@UnitId", unitId);

            cmd.Parameters.AddWithValue("@FiscalYear",
                v.Fy ?? (object)DBNull.Value);

            cmd.Parameters.AddWithValue("@FiscalPeriod",
                v.Fp ?? (object)DBNull.Value);

            cmd.Parameters.AddWithValue("@FormType", v.Form);
            cmd.Parameters.AddWithValue("@Accession",
                v.Accn ?? (object)DBNull.Value);

            var p =
                cmd.Parameters.Add("@Value", SqlDbType.Decimal);

            p.Precision = 18;
            p.Scale = 6;
            p.Value = value ?? (object)DBNull.Value;

            cmd.Parameters.Add("@FiledDate",
                SqlDbType.DateTime2)
                .Value = filed ?? (object)DBNull.Value;

            await cmd.ExecuteNonQueryAsync();
        }
    }
}
