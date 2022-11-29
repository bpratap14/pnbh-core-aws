using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace pnbh.core.aws
{
    public sealed class S3FileModel : S3FileInfoModel
    {
        public byte[] Bytes { get; set; }
    }
}
