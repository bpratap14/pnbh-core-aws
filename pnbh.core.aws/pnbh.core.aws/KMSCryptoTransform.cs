using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace pnbh.core.aws
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    internal abstract class KMSCryptoTransform : ICryptoTransform
    {
        private readonly IAmazonKeyManagementService _client;
        protected string _keyId;

        protected KMSCryptoTransform(IAmazonKeyManagementService client)
        {
            _client = client;
        }

        protected KMSCryptoTransform(IAmazonKeyManagementService client, string keyId)
            : this(client)
        {
            _keyId = keyId;
        }

        public bool CanReuseTransform => true;

        public bool CanTransformMultipleBlocks => false;

        public int InputBlockSize => throw new NotImplementedException();

        public int OutputBlockSize => throw new NotImplementedException();

        public int TransformBlock(byte[] inputBuffer, int inputOffset, int inputCount, byte[] outputBuffer,
            int outputOffset)
        {
            throw new NotImplementedException();
        }

        public abstract byte[] TransformFinalBlock(byte[] inputBuffer, int inputOffset, int inputCount);

        public void Dispose()
        {

        }

        public class Decryptor : KMSCryptoTransform
        {
            public Decryptor(IAmazonKeyManagementService client)
                : base(client)
            {
            }

            public override byte[] TransformFinalBlock(byte[] inputBuffer, int inputOffset, int inputCount)
            {
                return _client.Decrypt(new DecryptRequest()
                {
                    CiphertextBlob = new MemoryStream(inputBuffer, inputOffset, inputCount)
                }).Plaintext.ToArray();

            }
        }

        public class Encryptor : KMSCryptoTransform
        {
            public Encryptor(IAmazonKeyManagementService client, string keyId)
                : base(client, keyId)
            {
            }

            public override byte[] TransformFinalBlock(byte[] inputBuffer, int inputOffset, int inputCount)
            {
                return _client.Encrypt(new EncryptRequest()
                {
                    KeyId = _keyId,
                    Plaintext = new MemoryStream(inputBuffer, inputOffset, inputCount)
                }).CiphertextBlob.ToArray();
            }
        }
    }
}
