using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace pnbh.core.aws
{
    
    public class ApiResponse<T,TE> where TE : class
    {
        public bool IsSuccessStatusCode { get; set; }
        public int StatusCode { get; set; }
        public string ReasonPhrase { get; set; }
        public string MediaType { get; set; }
        public Type Type { get; set; }
        public T Result { get; set; }
        public TE ErrorResponse { get; set; }
    }
}
