using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
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
        /// <summary>
        /// Public method for copy to S3 Bucket from file
        /// </summary>
        /// <param name="fileName">Name of the file</param>
        /// <param name="s3Uri">S3Uri</param>
        public void CopyToS3(string fileName,string s3Uri)
        {
            Private_Copy(fileName, s3Uri);
        }
        /// <summary>
        /// Private method for copy to S3 Bucket from file
        /// </summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        private void Private_Copy(string source, string destination)
        {
            var src = source.Substring(0, 2).ToLower().Equals("s3");
            var dest = destination.Substring(0, 2).ToLower().Equals("s3");

            //copy from s3
            if (src)
            {
                if (!dest)
                {
                    var destFile = destination;
                    if (Directory.Exists(destination))
                    {
                        var s3ObjName = source.Substring(source.LastIndexOf("/", StringComparison.CurrentCulture) + 1);
                        destFile = Path.Combine(destination, s3ObjName);
                    }
                    using (var stream=new FileStream(destFile,FileMode.Create))
                    {
                        Private_Copy(source, stream);
                    }
                }
                else
                {
                    throw new NotImplementedException("Copy to S3 from S3,use some other utility. :-)");
                }
            }
            else
            {
                //copy to s3 from local
                if (dest)
                {
                    using (var stream = new FileStream(source, FileMode.Create))
                    {
                        Private_Copy(stream,destination);
                    }

                }
            }
        }
        /// <summary>
        /// Copy to s3 Bucket from a stream
        /// </summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        private void Private_Copy(Stream source, string destination)
        {
            var s3Bucket = destination.Substring(5, destination.IndexOf("/", 5, StringComparison.CurrentCulture) - 5);
            var s3Key = destination.Substring(destination.IndexOf("/", 5, StringComparison.CurrentCulture) + 1);
            try
            {
                using (var client=new AmazonS3Client())
                {
                    source.Position = 0;
                    client.PutObject(new PutObjectRequest()
                    {
                        BucketName = s3Bucket,
                        Key = s3Key,
                        InputStream = source
                    });
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null && (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") || amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message ' (0)' when reading an object",
                    amazonS3Exception.Message);
                }
            }
        }
        /// <summary>
        /// copy to s3 buckt from a stram
        /// </summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
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
                    //GetobjectResponse
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
                if (amazonS3Exception.ErrorCode != null && (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") || amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message ' (0)' when reading an object",
                    amazonS3Exception.Message);
                }
            }
        }
    }
}
