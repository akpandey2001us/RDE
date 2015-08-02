using System;
using System.Collections;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace RobbinsExportBusinessLayer
{
    public class DataDecryptionLayer
    {
        public static X509Certificate2 GetCurrentCertificate(string certThumb)
        {
            if (String.IsNullOrEmpty(certThumb))
            {
                return null;
            }

            X509Certificate2 certificate = GetSpecificCertificate(certThumb);

            if (certificate == null)
            {
                throw new Exception("Certificate not found");
            }

            return certificate;
        }

        private static X509Certificate2 GetSpecificCertificate(string thumbprint, bool validOnly = true)
        {
            if (String.IsNullOrEmpty(thumbprint))
            {
                return null;
            }

            X509Store store = new X509Store(StoreName.My, StoreLocation.LocalMachine);

            try
            {
                store.Open(OpenFlags.ReadOnly);

#if DEBUG
                // Have to enable invalid certificates for debugging to be able to find self signed certificates
                X509Certificate2Collection collection = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, false);
#else
                X509Certificate2Collection collection = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, validOnly);
#endif

                if (collection.Count > 0)
                {
                    return collection[0];
                }
                else
                {
                    throw new Exception("Certificate not found");
                }
            }
            finally
            {
                store.Close();
            }
        }

        public static string Decrypt(string inputString, X509Certificate2 certificate)
        {
            try
            {
                if (String.IsNullOrEmpty(inputString) || certificate == null)
                {
                    // not encrypted, so return the cypherText
                    return inputString;
                }

                var rsaCryptoServiceProvider
                    = (RSACryptoServiceProvider)certificate.PrivateKey;
                int dwKeySize = rsaCryptoServiceProvider.KeySize;
                int base64BlockSize = ((dwKeySize / 8) % 3 != 0)
                                          ? (((dwKeySize / 8) / 3) * 4) + 4
                                          : ((dwKeySize / 8) / 3) * 4;
                int iterations = inputString.Length / base64BlockSize;

                var arrayList = new ArrayList();
                for (int i = 0; i < iterations; i++)
                {
                    byte[] encryptedBytes = Convert.FromBase64String(
                        inputString.Substring(base64BlockSize * i, base64BlockSize));

                    // The RSACryptoServiceProvider reverses the order of 
                    // encrypted bytes after encryption and before decryption.
                    Array.Reverse(encryptedBytes);
                    arrayList.AddRange(rsaCryptoServiceProvider.Decrypt(
                        encryptedBytes, true));
                }
                return Encoding.UTF32.GetString(arrayList.ToArray(
                    Type.GetType("System.Byte")) as byte[]);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("The value specified could not be decrypted with the specified certificate, and may have been modified"), ex);
            }
        }
    }
}
