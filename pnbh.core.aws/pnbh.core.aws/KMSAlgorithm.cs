using Amazon.KeyManagementService;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace pnbh.core.aws
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    internal class KMSAlgorithm : SymmetricAlgorithm
    {
        private readonly IAmazonKeyManagementService _client;
        private readonly string _keyId;

        public KMSAlgorithm(IAmazonKeyManagementService client)
        {
            _client = client;
        }

        public KMSAlgorithm(IAmazonKeyManagementService client, string keyId)
            : this(client)
        {
            _keyId = keyId;
        }

        public override ICryptoTransform CreateDecryptor()
        {
            return new KMSCryptoTransform.Decryptor(_client);
        }

        public override ICryptoTransform CreateDecryptor(byte[] rgbKey, byte[] rgbIV)
        {
            throw new NotImplementedException();
        }

        public override ICryptoTransform CreateEncryptor()
        {
            return new KMSCryptoTransform.Encryptor(_client, _keyId);
        }

        public override ICryptoTransform CreateEncryptor(byte[] rgbKey, byte[] rgbIV)
        {
            throw new NotImplementedException();
        }

        public override void GenerateIV()
        {
            throw new NotImplementedException();
        }

        public override void GenerateKey()
        {
            throw new NotImplementedException();
        }
    }
}
