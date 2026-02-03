using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmerciaMarketSecFactWeb.Dto
{
    public class CompanyProfileDto
    {
        public string? Name { get; set; }
        public string? Country { get; set; }
        public string? Currency { get; set; }
        public string? Industry { get; set; }
        public decimal MarketCapitalization { get; set; }
        public decimal ShareOutstanding { get; set; }
        public DateTime? Ipo { get; set; }
    }
}
