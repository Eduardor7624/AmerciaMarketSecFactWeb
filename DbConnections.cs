using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmerciaMarketSecFactWeb
{
    public static class DbConnections
    {
        public const string AmericaMarket =
            "Data Source=AM1\\AMDEV;Initial Catalog=AmericaMarket;User ID=sa;Password=Rewq1234;TrustServerCertificate=True";

        public const string AmericaMarketSec =
            "Data Source=AM1\\AMDEV;Initial Catalog=AmericaMarketSec;User ID=sa;Password=Rewq1234;TrustServerCertificate=True";

        public const string AmericaMarketCommon =
            "Data Source=AM1\\AMDEV;Initial Catalog=AmericaMarketCommon;User ID=sa;Password=Rewq1234;TrustServerCertificate=True";
    }
}
