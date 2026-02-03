using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace AmerciaMarketSecFactWeb.SecEdgar
{
    public sealed class SecCompanyFactsDto
    {
        [JsonConverter(typeof(CikFlexibleConverter))]
        public string Cik { get; set; }
        public string EntityName { get; set; }

        // taxonomy -> concept -> ConceptDto
        public Dictionary<string, Dictionary<string, ConceptDto>> Facts { get; set; }
    }

    public sealed class ConceptDto
    {
        public string Label { get; set; }
        public string Description { get; set; }

        // unit -> list of values
        public Dictionary<string, List<ConceptUnitValueDto>> Units { get; set; }
    }

    public sealed class ConceptUnitValueDto
    {
        public decimal? Val { get; set; }
        public int? Fy { get; set; }
        public string Fp { get; set; }
        public string Form { get; set; }
        public DateTime? Filed { get; set; }
        public string Accn { get; set; }
        public string Frame { get; set; }
    }

    public sealed class CikFlexibleConverter : JsonConverter<string>
    {
        public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return reader.TokenType switch
            {
                JsonTokenType.String => reader.GetString(),
                JsonTokenType.Number => reader.GetInt64().ToString("D10"), // pad a 10
                _ => throw new JsonException($"Unsupported CIK token: {reader.TokenType}")
            };
        }

        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }
}
