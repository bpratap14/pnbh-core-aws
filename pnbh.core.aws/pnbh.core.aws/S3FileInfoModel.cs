using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace pnbh.core.aws
{
    
    public class S3FileInfoModel
    {
        
        public string Type { get; set; }
        public string Extension { get; set; }
        public string Name { get; set; }
        public string Path { get; set; }
        public string S3Uri { get; set; }
        public string Bucket { get; set; }
        public string Key { get; set; }
        public long Size { get; set; }
        public string CreateDtm { get; set; }
        public string LastModifiedDtm { get; set; }
    }
}
