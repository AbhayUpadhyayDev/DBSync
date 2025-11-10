using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBSyncService
{
    public static class AdvancedCompressor
    {
        public static byte[] CompressString(string str)
        {
            if (string.IsNullOrEmpty(str))
                return Array.Empty<byte>();
            string preprocessed = PreprocessString(str);
            var bytes = Encoding.UTF8.GetBytes(preprocessed);
            using var inputStream = new MemoryStream(bytes);
            using var outputStream = new MemoryStream();
            using (var brotliStream = new BrotliStream(outputStream, CompressionLevel.SmallestSize))
            {
                inputStream.CopyTo(brotliStream);
            }

            return outputStream.ToArray();
        }

        private static string PreprocessString(string input)
        {
            StringBuilder result = new StringBuilder();
            int i = 0;
            while (i < input.Length)
            {
                char current = input[i];
                int count = 1;
                while (i + 1 < input.Length && input[i + 1] == current)
                {
                    count++;
                    i++;
                }
                if (count > 3) 
                {
                    result.Append($"[{current}x{count}]");
                }
                else
                {
                    result.Append(current, count);
                }
                i++;
            }
            return result.ToString();
        }
        public static string DecompressString(byte[] compressed)
        {
            if (compressed == null || compressed.Length == 0)
                return string.Empty;

            using var inputStream = new MemoryStream(compressed);
            using var outputStream = new MemoryStream();
            using (var brotliStream = new BrotliStream(inputStream, CompressionMode.Decompress))
            {
                brotliStream.CopyTo(outputStream);
            }

            string preprocessed = Encoding.UTF8.GetString(outputStream.ToArray());
            return PostprocessString(preprocessed);
        }

        private static string PostprocessString(string input)
        {
            StringBuilder result = new StringBuilder();
            int i = 0;
            while (i < input.Length)
            {
                if (i < input.Length - 1 && input[i] == '[')
                {
                    int endIndex = input.IndexOf(']', i);
                    if (endIndex != -1)
                    {
                        string token = input.Substring(i + 1, endIndex - i - 1);
                        if (token.Contains("x"))
                        {
                            var parts = token.Split('x');
                            if (parts.Length == 2 && char.TryParse(parts[0], out char c) && int.TryParse(parts[1], out int count))
                            {
                                result.Append(c, count);
                                i = endIndex + 1;
                                continue;
                            }
                        }
                    }
                }
                result.Append(input[i]);
                i++;
            }
            return result.ToString();
        }
    }
}
