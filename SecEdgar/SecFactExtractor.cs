using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmerciaMarketSecFactWeb.SecEdgar
{
    public static class SecFactExtractor
    {
        public static decimal? GetLatestUsdValue(SecCompanyFactsDto dto,string taxonomy,string concept,string unit = "USD")
        {
            if (dto?.Facts == null) return null;

            // taxonomy -> (concept -> ConceptDto)
            if (!dto.Facts.TryGetValue(taxonomy, out var concepts) || concepts == null)
                return null;

            // concept -> ConceptDto
            if (!concepts.TryGetValue(concept, out var conceptDto) || conceptDto?.Units == null)
                return null;

            // unit -> list of values
            if (!conceptDto.Units.TryGetValue(unit, out var values) || values == null || values.Count == 0)
                return null;

            return values
                .Where(v => v?.Val != null)
                .OrderByDescending(v => v.Fy ?? int.MinValue)
                .ThenByDescending(v => v.Filed ?? DateTime.MinValue)
                .FirstOrDefault()
                ?.Val;
        }

    }
}
