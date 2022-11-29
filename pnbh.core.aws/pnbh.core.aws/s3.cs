using Amazon.KeyManagementService;
using Amazon.S3;
using Amazon.S3.Encryption;
using Amazon.S3.IO;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Hosting;

namespace pnbh.core.aws
{
    public class s3
    {
        static s3()
        {
            Configuration config = null;
            if (HostingEnvironment.IsHosted || Assembly.GetEntryAssembly() == null || HttpContext.Current != null && !HttpContext.Current.Request.PhysicalPath.Equals(string.Empty))
            {
                //IIS Does not like these channges..add will not run afterward
            }
            else
            {
                config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            }
            if (config==null)
            {
                return;
            }
            if (ConfigurationManager.AppSettings["AWSProfileName"]==null)
            {
                config.AppSettings.Settings.Add("AWSProfileName", "pnbh");
                config.AppSettings.SectionInformation.ForceSave = true;
                config.Save(ConfigurationSaveMode.Modified, true);
                ConfigurationManager.RefreshSection("appSettings");
            }
            if (ConfigurationManager.AppSettings["AWSRegion"] == null)
            {
                config.AppSettings.Settings.Add("AWSRegion", "us-east-1");
                config.AppSettings.SectionInformation.ForceSave = true;
                config.Save(ConfigurationSaveMode.Modified, true);
                ConfigurationManager.RefreshSection("appSettings");
            }
        }

        #region Public Method
        /// <summary>
        /// Copy to S3 Bucket from a file
        /// </summary>
        /// <param name="fileName">Name of the file</param>
        /// <param name="s3Uri">S3Uri</param>
        public void CopyToS3(string fileName, string s3Uri)
        {
            Private_Copy(fileName, s3Uri);
        }


        /// <summary>
        /// Copy to S3 Bucket from a byte[]
        /// </summary>
        /// <param name="source">byte[]</param>
        /// <param name="s3Uri">S3Uri</param>
        public void CopyToS3(byte[] source, string s3Uri)
        {
            using (Stream ms = new MemoryStream(source))
            {
                Private_Copy(ms, s3Uri);
            }

        }

        /// <summary>
        /// Copy to S3 Bucket from a byte[]
        /// </summary>
        /// <param name="source">byte[]</param>
        /// <param name="s3Uri">S3Uri</param>
        public void CopyToS3(byte[] source, string s3Uri, bool isPublic = false)
        {
            using (Stream ms = new MemoryStream(source))
            {
                if (isPublic)
                {
                    Public_Copy(ms, s3Uri);
                }
                else
                {
                    Private_Copy(ms, s3Uri);
                }
            }

        }

        /// <summary>
        /// Copy to S3 Bucket from a stream
        /// </summary>
        /// <param name="source">byte[]</param>
        /// <param name="s3Uri">S3Uri</param>
        public void CopyToS3(Stream source, string s3Uri)
        {
            Private_Copy(source, s3Uri);
        }


        /// <summary>
        /// Copy from S3 Bucket to local file
        /// </summary>
        /// <param name="s3Uri">S3Uri</param>
        /// <param name="path">Destination file name</param>
        public void CopyFromS3(string s3Uri, string path)
        {
            Private_Copy(s3Uri, path);
        }

        /// <summary>
        /// Copy from S3 Bucket to byte array
        /// </summary>
        /// <param name="s3Uri">S3Uri</param>
        public byte[] CopyFromS3(string s3Uri)
        {
            using (var ms = new MemoryStream())
            {
                Private_Copy(s3Uri, ms);
                return ms.ToArray();
            }
        }

        ///// <summary>
        ///// Copy a file from an S3 Bucket
        ///// </summary>
        ///// <param name="source">The S3Uri to the file to retrieve.</param>
        ///// <returns>byte[]</returns>>    
        //public byte[] GetFile(string source )
        //{
        //    using (var stream = new MemoryStream())
        //    {
        //        Private_Copy(source, stream);
        //        return stream.ToArray();
        //    }

        //}

        /// <summary>
        /// Copy a file from an S3 Bucket
        /// </summary>
        /// <param name="source">The source S3Uri to retrieve the file from.</param>
        /// <param name="stream">The destination stream to copy the file to.</param>
        /// <returns>byte[]</returns>>    
        public void GetFile(string source, Stream stream)
        {
            Private_Copy(source, stream);
        }

        /// <summary>
        /// Copy a file from an S3 Bucket
        /// </summary>
        /// <param name="source">The source S3Uri to retrieve the file from.</param>
        /// <param name="stream">The destination stream to copy the file to.</param>
        /// <returns>byte[]</returns>>    
        public void GetFileAsync(string source, Stream stream)
        {
            Private_CopyAsync(source, stream);
        }

        /// <summary>
        /// Encrypt to S3 Bucket from a file
        /// </summary>
        /// <param name="fileName">The name of the file to copy from </param>
        /// <param name="s3Uri">S3Uri</param>
        /// <param name="kmsKeyId">The KMS KeyId or Alias used to encrypt or decrypt the object</param>
        public void EncryptToS3(string fileName, string s3Uri, string kmsKeyId)
        {
            PrivateCopyEncrypt(fileName, s3Uri, kmsKeyId);
        }

        /// <summary>
        /// Encrypt to S3 Bucket from a byte[]
        /// </summary>
        /// <param name="source">byte[]</param>
        /// <param name="s3Uri">S3Uri</param>
        /// <param name="kmsKeyId">The KMS KeyId or Alias used to encrypt or decrypt the object</param>
        public void EncryptToS3(byte[] source, string s3Uri, string kmsKeyId)
        {
            using (Stream ms = new MemoryStream(source))
            {
                Private_Encrypt(ms, s3Uri, kmsKeyId);
            }

        }


        /// <summary>
        /// Decrypt from S3 Bucket to local file
        /// </summary>
        /// <param name="s3Uri">S3Uri</param>
        /// <param name="path">Destination file name</param>
        public void DecryptFromS3(string s3Uri, string path)
        {
            PrivateCopyEncrypt(s3Uri, path, null);
        }


        /// <summary>
        /// Copy from S3 Bucket to byte[]
        /// </summary>
        /// <param name="s3Uri">S3Uri</param>
        /// <returns>byte[]</returns>>    
        public byte[] DecryptFromS3(string s3Uri)
        {
            using (var stream = new MemoryStream())
            {
                Private_Decrypt(s3Uri, stream);
                return stream.ToArray();
            }
        }


        /// <summary>Gets a list of files in the specified s3Uri.</summary>
        /// <param name="source">The s3Uri to search.</param>
        /// <param name="searchPattern">The search string. The default pattern is "*", which returns all files in the specified folder.</param>
        /// <returns>A list of FileInfoModel for the files in the specified AWS s3Uri, or an empty list if no files are found.</returns>
        public ApiResponse<List<S3FileInfoModel>, ApplicationErrorModel> GetFiles(string source, string searchPattern = "*")
        {
            return Private_GetFiles(source, searchPattern, SearchOption.TopDirectoryOnly);
        }

        /// <summary>Gets a list of all files in the specified s3Uri.</summary>
        /// <param name="source">The s3Uri to search.</param>
        /// <param name="searchOption">One of the enumeration values that specifies whether the search operation should include only the current directory or all subdirectories. The default value is TopDirectoryOnly.</param>
        /// <returns>A list of FileInfoModel for the files in the specified AWS s3Uri, or an empty list if no files are found.</returns>
        public ApiResponse<List<S3FileInfoModel>, ApplicationErrorModel> GetFiles(string source, SearchOption searchOption)
        {
            if (!Enum.IsDefined(typeof(SearchOption), searchOption))
                throw new InvalidEnumArgumentException(nameof(searchOption), (int)searchOption, typeof(SearchOption));

            return Private_GetFiles(source, "*", searchOption);
        }

        /// <summary>Gets a list of files in the specified s3Uri.</summary>
        /// <param name="source">The s3Uri to search.</param>
        /// <param name="searchPattern">The search string. The default pattern is "*", which returns all files.</param>
        /// <param name="searchOption">One of the enumeration values that specifies whether the search operation should include only the current directory or all subdirectories. The default value is TopDirectoryOnly.</param>
        /// <returns>A list of FileInfoModel for the files in the specified AWS s3Uri, or an empty list if no files are found.</returns>
        public ApiResponse<List<S3FileInfoModel>, ApplicationErrorModel> GetFiles(string source, string searchPattern, SearchOption searchOption)
        {
            if (!Enum.IsDefined(typeof(SearchOption), searchOption))
                throw new InvalidEnumArgumentException(nameof(searchOption), (int)searchOption, typeof(SearchOption));

            return Private_GetFiles(source, searchPattern, searchOption);
        }

        public ApiResponse<bool, ApplicationErrorModel> MoveFile(string source, string destination, bool overwrite)
        {
            var returnValue = new ApiResponse<bool, ApplicationErrorModel> { Type = typeof(bool) };

            source = HttpUtility.UrlDecode(source);

            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var srcS3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var srcS3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1, source.LastIndexOf("/", StringComparison.Ordinal) - 6 - srcS3Bucket.Length);
            var srcFileName = source.Substring(source.LastIndexOf("/", StringComparison.Ordinal) + 1);

            var destS3Bucket = destination.Substring(5, destination.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var destS3Key = destination.Substring(destination.IndexOf("/", 5, StringComparison.CurrentCulture) + 1, destination.LastIndexOf("/", StringComparison.Ordinal) - 6 - destS3Bucket.Length);
            var destFileName = destination.Substring(destination.LastIndexOf("/", StringComparison.Ordinal) + 1);


            try
            {
                using (var client = new AmazonS3Client())
                {
                    //odd that SDK doesn't support native directory separator...
                    srcS3Key = srcS3Key.Replace("/", @"\");
                    destS3Key = destS3Key.Replace("/", @"\");

                    var dir = new S3DirectoryInfo(client, srcS3Bucket, srcS3Key);

                    var response = dir.GetFiles(srcFileName, SearchOption.TopDirectoryOnly);

                    if (response.Length == 0)
                    {
                        returnValue.StatusCode = (int)HttpStatusCode.NotFound;
                        returnValue.ReasonPhrase = "FILE_NOT_FOUND";
                        return returnValue;
                    }
                    else if (response.Length > 1)
                    {
                        returnValue.StatusCode = (int)HttpStatusCode.Conflict;
                        returnValue.ReasonPhrase = "MULTIPLE_FILES_FOUND";
                        return returnValue;

                    }
                    else
                    {
                        var desFileInfo = new S3FileInfo(client, destS3Bucket, $@"{destS3Key}\{destFileName}");
                        if (overwrite && desFileInfo.Exists)
                        {
                            response[0].Replace(desFileInfo, null);

                        }
                        else
                        {
                            response[0].MoveTo(desFileInfo);
                        }


                        returnValue.StatusCode = (int)HttpStatusCode.OK;
                        returnValue.Result = true;
                    }
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    returnValue.StatusCode = (int)HttpStatusCode.Unauthorized;
                    returnValue.ReasonPhrase = "UNAUTHORIZED";

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };
                }
                else
                {
                    returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                    returnValue.ReasonPhrase = amazonS3Exception.ErrorCode;

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };

                    //Console.WriteLine($"An error occurred with the message '{amazonS3Exception.Message}' when reading an object");
                }
            }
            catch (Exception e)
            {
                returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                returnValue.ReasonPhrase = e.Message;

                //Console.WriteLine($"{e}\r\n{e.StackTrace}");
            }
            return returnValue;
        }



        /// <summary>
        /// Moves a file from S3 Bucket to another S3 bucket
        /// </summary>
        /// <param name="source">The source S3 path.</param>
        /// <param name="destination">The destination S3 path.</param>
        /// <param name="overwrite">If the file exists it will be overwritten.</param>
        /// <returns></returns>
        public ApiResponse<string, ApplicationErrorModel> ReadFile(string source)
        {
            var returnValue = new ApiResponse<string, ApplicationErrorModel> { Type = typeof(string) };

            source = HttpUtility.UrlDecode(source);
            String content = string.Empty;
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var srcS3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var srcS3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1, source.LastIndexOf("/", StringComparison.Ordinal) - 6 - srcS3Bucket.Length);
            var srcFileName = source.Substring(source.LastIndexOf("/", StringComparison.Ordinal) + 1);

            try
            {
                using (var client = new AmazonS3Client())
                {
                    var request = new GetObjectRequest()
                    {
                        BucketName = srcS3Bucket,
                        Key = source
                    };
                    using (var response = client.GetObject(request))
                    {
                        StreamReader reader = new StreamReader(response.ResponseStream);
                        content = reader.ReadToEnd();
                        reader.Close();
                        returnValue.Result = content;
                        returnValue.StatusCode = (int)HttpStatusCode.OK;
                        return returnValue;
                    }
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    returnValue.StatusCode = (int)HttpStatusCode.Unauthorized;
                    returnValue.ReasonPhrase = "UNAUTHORIZED";

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };
                }
                else
                {
                    returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                    returnValue.ReasonPhrase = amazonS3Exception.ErrorCode;

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };

                    //Console.WriteLine($"An error occurred with the message '{amazonS3Exception.Message}' when reading an object");
                }
            }
            catch (Exception e)
            {
                returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                returnValue.ReasonPhrase = e.Message;

                //Console.WriteLine($"{e}\r\n{e.StackTrace}");
            }
            return returnValue;
        }
        #endregion Public Methods

        #region Private Methods
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        private void Private_Copy(string source, string destination)
        {
            var srcS3 = source.Substring(0, 2).ToLower().Equals("s3");
            var destS3 = destination.Substring(0, 2).ToLower().Equals("s3");

            //copy from S3
            if (srcS3)
            {
                //copy from S3 bucket to local
                if (!destS3)
                {
                    var destFile = destination;


                    if (Directory.Exists(destination))
                    {
                        //determine if destination is a path or a file name
                        //if the destination is a folder, it has to exist first.
                        //We don't want to speculate if the user wanted to create
                        //a folder or a file without an extension.
                        //if it is a folder, use the S3 object name

                        var s3ObjectName =
                            source.Substring(source.LastIndexOf("/", StringComparison.CurrentCulture) + 1);
                        destFile = Path.Combine(destination, s3ObjectName);
                    }

                    using (var stream = new FileStream(destFile, FileMode.Create))
                    {
                        Private_Copy(source, stream);
                    }

                }
                else
                {
                    //S3 to S3
                    //TODO: MAYBE:
                    throw new NotImplementedException("Copy to S3 from S3, use some other utility. :-)");
                }


            }
            else
            {
                //copy to S3 from local
                if (destS3)
                {
                    using (var stream = new FileStream(source, FileMode.Open))
                    {
                        Private_Copy(stream, destination);
                    }
                }
                else
                {
                    throw new NotImplementedException("Copy to local from local, use some other utility. :-)");
                }
            }
        }

        /// <summary>
        /// Copy to S3 Bucket from a stream
        /// </summary>
        /// <param name="source">Stream</param>
        /// <param name="destination">S3Uri</param>
        private void Private_Copy(Stream source, string destination)
        {

            var s3Bucket = destination.Substring(5, destination.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = destination.Substring(destination.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);

            try
            {
                using (var client = new AmazonS3Client())
                {
                    //make sure we are at the beginning of the stream 
                    source.Position = 0;

                    client.PutObject(new PutObjectRequest()
                    {
                        BucketName = s3Bucket,
                        Key = s3Key,
                        InputStream = source,
                    });


                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                        amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when reading an object",
                        amazonS3Exception.Message);
                }
            }

        }

        /// <summary>
        /// Copy to S3 Bucket from a stream with Public Read Access
        /// </summary>
        /// <param name="source">Stream</param>
        /// <param name="destination">S3Uri</param>
        private void Public_Copy(Stream source, string destination)
        {

            var s3Bucket = destination.Substring(5, destination.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = destination.Substring(destination.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);

            try
            {
                using (var client = new AmazonS3Client())
                {
                    //make sure we are at the beginning of the stream 
                    source.Position = 0;

                    client.PutObject(new PutObjectRequest()
                    {
                        BucketName = s3Bucket,
                        Key = s3Key,
                        InputStream = source,
                        CannedACL = S3CannedACL.PublicRead
                    });
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                        amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when reading an object",
                        amazonS3Exception.Message);
                }
            }

        }

        /// <summary>
        /// Copy a file from an S3 Bucket to a stream.
        /// </summary>
        /// <param name="source">S3Uri</param>
        /// <param name="destination">Stream</param>
        private void Private_Copy(string source, Stream destination)
        {

            var s3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);

            try
            {
                using (var client = new AmazonS3Client())
                {
                    var request = new GetObjectRequest()
                    {
                        BucketName = s3Bucket,
                        Key = s3Key
                    };

                    //GetObjectResponse
                    using (var response = client.GetObject(request))
                    {
                        if (response == null)
                            return;

                        using (var srcStream = response.ResponseStream)
                        {
                            if (srcStream == null)
                                return;

                            using (new StreamWriter(destination))
                            {
                                srcStream.CopyTo(destination);
                            }
                        }
                    }

                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when reading an object",
                        amazonS3Exception.Message);
                }
            }

        }


        /// <summary>
        /// Copy a file from an S3 Bucket to a stream.
        /// </summary>
        /// <param name="source">S3Uri</param>
        /// <param name="destination">Stream</param>
        private async void Private_CopyAsync(string source, Stream destination)
        {
            source = HttpUtility.UrlDecode(source);

            if (source == null)
                throw new ArgumentNullException(nameof(source));


            var s3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);

            try
            {
                using (var client = new AmazonS3Client())
                {
                    var request = new GetObjectRequest()
                    {
                        BucketName = s3Bucket,
                        Key = s3Key
                    };

                    using (var response = await client.GetObjectAsync(request))
                    {
                        if (response == null)
                            return;

                        using (var srcStream = response.ResponseStream)
                        {
                            if (srcStream == null)
                                return;

                            using (new StreamWriter(destination))
                            {
                                await srcStream.CopyToAsync(destination);
                            }
                        }
                    }

                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when reading an object",
                        amazonS3Exception.Message);
                }
            }

        }
        /////<summary>
        ///// List objects in an S3 Bucket
        ///// </summary>
        ///// <param name="source">S3Uri</param>
        //private List<S3Object> ls(string source)
        //{

        //    var s3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
        //    var s3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);

        //    try
        //    {
        //        using (var client = new AmazonS3Client())
        //        {
        //            var request = new ListObjectsRequest
        //            {
        //                BucketName = s3Bucket,
        //                Prefix = s3Key
        //            };

        //            var response = client.ListObjects(request);

        //            return response?.S3Objects;
        //        }
        //    }
        //    catch (AmazonS3Exception amazonS3Exception)
        //    {
        //        if (amazonS3Exception.ErrorCode != null &&
        //            (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
        //             amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
        //        {
        //            Console.WriteLine("Please check the provided AWS Credentials.");
        //        }
        //        else
        //        {
        //            Console.WriteLine("An error occurred with the message '{0}' when reading an object",
        //                amazonS3Exception.Message);
        //        }
        //    }

        //    return null;
        //}


        /// <summary>
        ///  Gets objects in an S3 Bucket
        ///  </summary>
        ///  <param name="source">S3Uri</param>
        /// <param name="searchPattern"></param>
        /// <param name="searchOption"></param>
        private ApiResponse<List<S3FileInfoModel>, ApplicationErrorModel> Private_GetFiles(string source, string searchPattern, SearchOption searchOption)
        {
            source = HttpUtility.UrlDecode(source);

            if (source == null)
                throw new ArgumentNullException(nameof(source));


            var s3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);
            var returnValue = new ApiResponse<List<S3FileInfoModel>, ApplicationErrorModel> { Type = typeof(List<S3FileInfoModel>) };

            try
            {
                using (var client = new AmazonS3Client())
                {
                    //odd that SDK doesn't support native directory separator...
                    s3Key = s3Key.Replace("/", @"\");

                    //s3 converts searchPattern to RegEx so we need to strip some chars.
                    searchPattern = searchPattern.TrimStart('^').TrimEnd('$');

                    var dir = new S3DirectoryInfo(client, s3Bucket, s3Key);

                    var response = dir.GetFiles(searchPattern, searchOption);

                    if (response.Length < 1)
                    {
                        returnValue.StatusCode = (int)HttpStatusCode.NotFound;
                    }
                    else
                    {

                        var fList = new List<S3FileInfoModel>();

                        for (var i = 0; i < response.Length; i++)
                        {
                            fList.Add(new S3FileInfoModel
                            {
                                CreateDtm = response[i].LastWriteTime.ToString(CultureInfo.InvariantCulture),
                                LastModifiedDtm = response[i].LastWriteTime.ToString(CultureInfo.InvariantCulture),
                                Name = response[i].Name,
                                Extension = response[i].Extension,
                                Path = response[i].Directory.FullName,
                                Type = response[i].Type.ToString(),
                                Size = response[i].Length,
                                Bucket = s3Bucket,
                                Key = s3Key,
                                S3Uri = $"{source}/{response[i].Name}"
                            });
                        }

                        returnValue.Result = fList;
                        returnValue.StatusCode = (int)HttpStatusCode.OK;
                    }
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    returnValue.StatusCode = (int)HttpStatusCode.Unauthorized;
                    returnValue.ReasonPhrase = "UNAUTHORIZED";

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };

                }
                else
                {
                    returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                    returnValue.ReasonPhrase = amazonS3Exception.ErrorCode;

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };
                }
            }
            catch (Exception e)
            {
                returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                returnValue.ReasonPhrase = e.Message;
            }

            return returnValue;
        }

        /// <summary>
        ///  Gets information about a file in an an S3 Bucket
        ///  </summary>
        ///  <param name="source">S3Uri</param>
        /// <param name="withContent">If true the file contents are also returned.</param>
        public ApiResponse<S3FileModel, ApplicationErrorModel> GetFile(string source, bool withContent)
        {
            

            source = HttpUtility.UrlDecode(source);

            if (source == null)
                throw new ArgumentNullException(nameof(source));


            var s3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1, source.LastIndexOf("/", StringComparison.Ordinal) - 6 - s3Bucket.Length);
            var fileName = source.Substring(source.LastIndexOf("/", StringComparison.Ordinal) + 1);

            var returnValue = new ApiResponse<S3FileModel, ApplicationErrorModel> { Type = typeof(S3FileModel) };

            try
            {
                using (var client = new AmazonS3Client())
                {
                    //odd that SDK doesn't support native directory separator...
                    s3Key = s3Key.Replace("/", @"\");


                    var dir = new S3DirectoryInfo(client, s3Bucket, s3Key);

                    var response = dir.GetFiles(fileName, SearchOption.TopDirectoryOnly);

                    if (response.Length == 0)
                    {
                        returnValue.StatusCode = (int)HttpStatusCode.NotFound;
                        returnValue.ReasonPhrase = "FILE_NOT_FOUND";
                        return returnValue;
                    }

                    if (response.Length > 1)
                    {
                        returnValue.StatusCode = (int)HttpStatusCode.Conflict;
                        returnValue.ReasonPhrase = "MULTIPLE_FILES_FOUND";
                        return returnValue;

                    }

                    var s3FileModel = new S3FileModel
                    {
                        CreateDtm = response[0].LastWriteTime.ToString(CultureInfo.InvariantCulture),
                        LastModifiedDtm = response[0].LastWriteTime.ToString(CultureInfo.InvariantCulture),
                        Name = response[0].Name,
                        Extension = response[0].Extension,
                        Path = response[0].Directory.FullName,
                        Type = response[0].Type.ToString(),
                        Size = response[0].Length,
                        Bucket = s3Bucket,
                        Key = s3Key,
                        S3Uri = source
                    };

                    if (withContent)
                    {
                        using (var stream = new MemoryStream())
                        {
                            Private_Copy(source, stream);
                            s3FileModel.Bytes = stream.ToArray();
                        }
                    }

                    returnValue.Result = s3FileModel;
                    returnValue.StatusCode = (int)HttpStatusCode.OK;
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {


                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    returnValue.StatusCode = (int)HttpStatusCode.Unauthorized;
                    returnValue.ReasonPhrase = "UNAUTHORIZED";

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };
                }
                else
                {
                    returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                    returnValue.ReasonPhrase = amazonS3Exception.ErrorCode;

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };
                }
            }
            catch (Exception e)
            {
                returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                returnValue.ReasonPhrase = e.Message;

                //Console.WriteLine($"{e}\r\n{e.StackTrace}");
            }

            return returnValue;
        }


        /// <summary>
        ///  Gets information about a file in an an S3 Bucket, and optionally returning the content
        ///  </summary>
        ///  <param name="source">S3Uri</param>
        /// <param name="withContent">If true the file contents are also returned.</param>
        public async Task<ApiResponse<S3FileModel, ApplicationErrorModel>> GetFileAsync(string source, bool withContent)
        {
            

            source = HttpUtility.UrlDecode(source);

            if (source == null)
                throw new ArgumentNullException(nameof(source));


            var s3Bucket = source.Substring(5, source.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1, source.LastIndexOf("/", StringComparison.Ordinal) - 6 - s3Bucket.Length);
            var fileName = source.Substring(source.LastIndexOf("/", StringComparison.Ordinal) + 1);

            var s3Key2 = source.Substring(source.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);


            var returnValue = new ApiResponse<S3FileModel, ApplicationErrorModel> { Type = typeof(S3FileModel) };

            try
            {
                using (var client = new AmazonS3Client())
                {
                    //odd that SDK doesn't support native directory separator...
                    s3Key = s3Key.Replace("/", @"\");
                    //s3Key2 = s3Key2.Replace("/", @"\");



                    var dir = new S3DirectoryInfo(client, s3Bucket, s3Key);

                    var response = dir.GetFiles(fileName, SearchOption.TopDirectoryOnly);

                    if (response.Length == 0)
                    {
                        returnValue.StatusCode = (int)HttpStatusCode.NotFound;
                        returnValue.ReasonPhrase = "FILE_NOT_FOUND";
                        return returnValue;
                    }

                    if (response.Length > 1)
                    {
                        returnValue.StatusCode = (int)HttpStatusCode.Conflict;
                        returnValue.ReasonPhrase = "MULTIPLE_FILES_FOUND";
                        return returnValue;

                    }

                    var s3FileModel = new S3FileModel
                    {
                        CreateDtm = response[0].LastWriteTime.ToString(CultureInfo.InvariantCulture),
                        LastModifiedDtm = response[0].LastWriteTime.ToString(CultureInfo.InvariantCulture),
                        Name = response[0].Name,
                        Extension = response[0].Extension,
                        Path = response[0].Directory.FullName,
                        Type = response[0].Type.ToString(),
                        Size = response[0].Length,
                        Bucket = s3Bucket,
                        Key = s3Key,
                        S3Uri = source
                    };


                    if (withContent)
                    {
                        var gor = new GetObjectRequest
                        {
                            BucketName = s3Bucket,
                            Key = s3Key2
                        };

                        var o = await client.GetObjectAsync(gor).ConfigureAwait(false);
                        using (var ms = new MemoryStream())
                        {
                            await o.ResponseStream.CopyToAsync(ms).ConfigureAwait(false);
                            s3FileModel.Bytes = ms.ToArray();
                        }
                    }


                    returnValue.Result = s3FileModel;
                    returnValue.StatusCode = (int)HttpStatusCode.OK;
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {


                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    returnValue.StatusCode = (int)HttpStatusCode.Unauthorized;
                    returnValue.ReasonPhrase = "UNAUTHORIZED";

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };
                }
                else
                {
                    returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                    returnValue.ReasonPhrase = amazonS3Exception.ErrorCode;

                    returnValue.ErrorResponse = new ApplicationErrorModel
                    {
                        ErrorCode = amazonS3Exception.ErrorCode,
                        ErrorMessage = amazonS3Exception.Message
                    };
                }
            }
            catch (Exception e)
            {
                returnValue.StatusCode = (int)HttpStatusCode.InternalServerError;
                returnValue.ReasonPhrase = e.Message;

                //Console.WriteLine($"{e}\r\n{e.StackTrace}");
            }

            return returnValue;
        }





        /// <summary>
        /// Copies a local file or S3 object to another location locally or in S3.
        /// http://docs.aws.amazon.com/cli/latest/reference/s3/cp.html
        /// https://stackoverflow.com/questions/15052390/reading-text-from-amazon-s3-stream
        /// </summary>
        /// <param name="source">The source to copy from. Can be a local file or an S3 Uri</param>
        /// <param name="destination">The destination to copy to. Can be a local file or an S3 Uri</param>
        /// <param name="kmsKeyId">The KMS KeyId or Alias used to encrypt or decrypt the object</param>
        private void PrivateCopyEncrypt(string source, string destination, string kmsKeyId)
        {
            source = HttpUtility.UrlDecode(source);

            if (source == null)
                throw new ArgumentNullException(nameof(source));

            destination = HttpUtility.UrlDecode(destination);

            if (destination == null)
                throw new ArgumentNullException(nameof(destination));


            var srcS3 = source.Substring(0, 2).ToLower().Equals("s3");
            var destS3 = destination.Substring(0, 2).ToLower().Equals("s3");

            //copy from S3
            if (srcS3)
            {
                //copy from S3 bucket to local
                if (!destS3)
                {
                    var destFile = destination;
                    if (Directory.Exists(destination))
                    {
                        //determine if destination is a path or a file name
                        //if the destination is a folder, it has to exist first.
                        //We don't want to speculate if the user wanted to create
                        //a folder or a file without an extension.
                        //if it is a folder, use the S3 object name

                        var s3ObjectName =
                            source.Substring(source.LastIndexOf("/", StringComparison.CurrentCulture) + 1);
                        destFile = Path.Combine(destination, s3ObjectName);
                    }

                    using (var stream = new FileStream(destFile, FileMode.Create))
                    {
                        Private_Decrypt(source, stream);
                    }

                }
                else
                {
                    //S3 to S3
                    //TODO: MAYBE:
                    throw new NotImplementedException("Copy to S3 from S3, use some other utility. :-)");
                }


            }
            else
            {
                //copy to S3 from local
                if (destS3)
                {
                    using (var stream = new FileStream(source, FileMode.Open))
                    {
                        Private_Encrypt(stream, destination, kmsKeyId);
                    }
                }
                else
                {
                    throw new NotImplementedException("Copy to local from local, use some other utility. :-)");
                }
            }
        }

        /// <summary>
        /// Encrypt to S3 Bucket from a stream
        /// </summary>
        /// <param name="source">Stream</param>
        /// <param name="destination">S3Uri</param>
        /// <param name="kmsKeyId">The KMS KeyId or Alias used to encrypt or decrypt the object</param>
        private void Private_Encrypt(Stream source, string destination, string kmsKeyId)
        {
            destination = HttpUtility.UrlDecode(destination);

            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            var s3Bucket = destination.Substring(5, destination.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = destination.Substring(destination.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);

            try
            {
                using (var client = new AmazonKeyManagementServiceClient())
                {
                    using (var algorithm = new KMSAlgorithm(client, kmsKeyId))
                    {
                        var materials = new EncryptionMaterials(algorithm);
                        var s3client = new AmazonS3EncryptionClient(materials);

                        s3client.PutObject(new PutObjectRequest()
                        {
                            BucketName = s3Bucket,
                            Key = s3Key,
                            InputStream = source
                        });

                    }
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when reading an object",
                        amazonS3Exception.Message);
                }
            }

        }


        /// <summary>
        /// Decrypt from S3 Bucket to a stream
        /// </summary>
        /// <param name="s3Uri"></param>
        /// <param name="destination">Stream</param>
        private void Private_Decrypt(string s3Uri, Stream destination)
        {
            s3Uri = HttpUtility.UrlDecode(s3Uri);

            if (s3Uri == null)
                throw new ArgumentNullException(nameof(s3Uri));


            var s3Bucket = s3Uri.Substring(5, s3Uri.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = s3Uri.Substring(s3Uri.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);

            try
            {
                using (var client = new AmazonKeyManagementServiceClient())
                {
                    using (var algorithm = new KMSAlgorithm(client))
                    {
                        var materials = new EncryptionMaterials(algorithm);
                        var s3client = new AmazonS3EncryptionClient(materials);

                        var request = new GetObjectRequest
                        {
                            BucketName = s3Bucket,
                            Key = s3Key
                        };


                        using (var response = s3client.GetObject(request))
                        {
                            using (var srcStream = response.ResponseStream)
                            {
                                if (srcStream == null)
                                    return;

                                using (new StreamWriter(destination))
                                {
                                    srcStream.CopyTo(destination);
                                }
                            }

                        }
                    }

                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when reading an object",
                        amazonS3Exception.Message);
                }
            }

        }

        #endregion Private Methods 
        /// <summary>
        /// Moves a file from S3 Bucket to another S3 bucket
        /// </summary>
        /// <param name="source">The source S3 path.</param>
        /// <param name="destination">The destination S3 path.</param>
        /// <param name="overwrite">If the file exists it will be overwritten.</param>
        /// <returns></returns>
        

    }
}
