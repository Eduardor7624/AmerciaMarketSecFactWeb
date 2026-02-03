using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmerciaMarketSecFactWeb
{

    public static class SecLog
    {
        public static async Task LogAsync(string? request, string? response, DateTime? sent, DateTime? received, string? exception, string? process)
        {
         
            try
            {
                using var conn = new SqlConnection(DbConnections.AmericaMarketSec);
                await conn.OpenAsync();

                var sql = @"
                            INSERT INTO SecLog
                            (
                                Process,
                                Request,
                                Response,
                                DateTimeSent,
                                DateTimeReceived,
                                ProcessException
                            )
                            VALUES
                            (
                                @Process,
                                @Request,
                                @Response,
                                @Sent,
                                @Received,
                                @Exception
                            );";

                using var cmd = new SqlCommand(sql, conn);

                cmd.Parameters.AddWithValue("@Process",
                     (object?)process ?? DBNull.Value);

                cmd.Parameters.AddWithValue("@Request",
                    (object?)request ?? DBNull.Value);

                cmd.Parameters.AddWithValue("@Response",
                    (object?)response ?? DBNull.Value);

                cmd.Parameters.AddWithValue("@Sent",
                    (object?)sent ?? DBNull.Value);

                cmd.Parameters.AddWithValue("@Received",
                    (object?)received ?? DBNull.Value);

                cmd.Parameters.AddWithValue("@Exception",
                    (object?)exception ?? DBNull.Value);

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("SEC LOG ERROR:");
                Console.WriteLine(ex.ToString());
            }
        }
    }
}