using Newtonsoft.Json;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace COME.Utilities
{
    static class Helper_Static
    {

        public static decimal TruncateDecimal(this decimal input, int precision = 8)
        {
            var temp = (decimal)Math.Pow(10, precision);
            //return decimal.Truncate(input * temp) / temp;
            temp = decimal.Truncate(input * temp) / temp;
            return temp;

        }
        public static string SerializeObject(this object myObject, bool isFormattingIntended = true, bool isCamelCase = false) => JsonConvert.SerializeObject(myObject, isFormattingIntended ? Formatting.Indented : Formatting.None, new JsonSerializerSettings { ReferenceLoopHandling = ReferenceLoopHandling.Ignore, NullValueHandling = NullValueHandling.Ignore });

    }
    public class Helper
    {
    }
}
