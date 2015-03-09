using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Common.Logging;
using Npgsql.Localization;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql
{
    ///<summary>
        /// This class provides many util methods to handle
        /// reading and writing of PostgreSQL protocol messages.
        /// </summary>
    internal static partial class PGUtil
    {
        public static async Task<int> ReadBytesAsync(this Stream stream, byte[] buffer, int offset, int count)
        {
            int end = offset + count;
            int got = 0;
            int need = count;
            do
            {
                got = (await stream.ReadAsync(buffer, offset, need));
                offset += got;
                need -= got;
            }
            while (offset < end && got != 0);
            // return bytes read
            return count - (end - offset);
        }

        public static async Task<Stream> WriteStringAsync(this Stream stream, String theString)
        {
            _log.Trace("Sending: " + theString);
            byte[] bytes = BackendEncoding.UTF8Encoding.GetBytes(theString);
            await stream.WriteAsync(bytes, 0, bytes.Length);
            return stream;
        }

        public static async Task<Stream> WriteStringAsync(this Stream stream, String format, params object[] parameters)
        {
            string theString = string.Format(format, parameters);
            _log.Trace("Sending: " + theString);
            byte[] bytes = BackendEncoding.UTF8Encoding.GetBytes(theString);
            await stream.WriteAsync(bytes, 0, bytes.Length);
            return stream;
        }

        public static async Task<Stream> WriteStringNullTerminatedAsync(this Stream stream, String theString)
        {
            _log.Trace("Sending: " + theString);
            byte[] bytes = BackendEncoding.UTF8Encoding.GetBytes(theString);
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Write(ASCIIByteArrays.Byte_0, 0, 1);
            return stream;
        }

        public static async Task<Stream> WriteStringNullTerminatedAsync(this Stream stream, String format, params object[] parameters)
        {
            string theString = string.Format(format, parameters);
            _log.Trace("Sending: " + theString);
            byte[] bytes = BackendEncoding.UTF8Encoding.GetBytes(theString);
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Write(ASCIIByteArrays.Byte_0, 0, 1);
            return stream;
        }

        public static async Task<Stream> WriteByteAsync(this Stream stream, byte[] b)
        {
            _log.Trace("Sending byte: {0}" + b[0]);
            await stream.WriteAsync(b, 0, 1);
            return stream;
        }

        public static async Task<Stream> WriteByteNullTerminatedAsync(this Stream stream, byte[] b)
        {
            _log.Trace("Sending byte: " + b[0]);
            await stream.WriteAsync(b, 0, 1);
            stream.Write(ASCIIByteArrays.Byte_0, 0, 1);
            return stream;
        }

        public static async Task<Stream> WriteBytesAsync(this Stream stream, byte[] the_bytes)
        {
            _log.Trace("Sending bytes: " + String.Join(", ", the_bytes));
            await stream.WriteAsync(the_bytes, 0, the_bytes.Length);
            return stream;
        }

        public static async Task<Stream> WriteBytesNullTerminatedAsync(this Stream stream, byte[] the_bytes)
        {
            _log.Trace("Sending bytes: " + String.Join(", ", the_bytes));
            await stream.WriteAsync(the_bytes, 0, the_bytes.Length);
            stream.Write(ASCIIByteArrays.Byte_0, 0, 1);
            return stream;
        }

        public static async Task<Stream> WriteLimStringAsync(this Stream network_stream, String the_string, Int32 n)
        {
            //Note: We do not know the size in bytes until after we have converted the string.
            byte[] bytes = BackendEncoding.UTF8Encoding.GetBytes(the_string);
            if (bytes.Length > n)
            {
                throw new ArgumentOutOfRangeException("the_string", the_string, string.Format("LimString write too large {0} {1}", the_string, n));
            }

            await network_stream.WriteAsync(bytes, 0, bytes.Length);
            //pad with zeros.
            if (bytes.Length < n)
            {
                bytes = new byte[n - bytes.Length];
                await network_stream.WriteAsync(bytes, 0, bytes.Length);
            }

            return network_stream;
        }

        public static async Task<Stream> WriteLimBytesAsync(this Stream network_stream, byte[] bytes, Int32 n)
        {
            if (bytes.Length > n)
            {
                throw new ArgumentOutOfRangeException("bytes", bytes, string.Format("LimString write too large {0} {1}", bytes, n));
            }

            await network_stream.WriteAsync(bytes, 0, bytes.Length);
            //pad with zeros.
            if (bytes.Length < n)
            {
                bytes = new byte[n - bytes.Length];
                await network_stream.WriteAsync(bytes, 0, bytes.Length);
            }

            return network_stream;
        }

        public static async Task CheckedStreamReadAsync(this Stream stream, Byte[] buffer, Int32 offset, Int32 size)
        {
            Int32 bytes_from_stream = 0;
            Int32 total_bytes_read = 0;
            // need to read in chunks so the socket doesn't run out of memory in recv
            // the network stream doesn't prevent this and downloading a large bytea
            // will throw an IOException with an error code of 10055 (WSAENOBUFS)
            int maxReadChunkSize = 8192;
            while (size > 0)
            {
                // chunked read of maxReadChunkSize
                int readSize = (size > maxReadChunkSize) ? maxReadChunkSize : size;
                bytes_from_stream = (await stream.ReadAsync(buffer, offset + total_bytes_read, readSize));
                total_bytes_read += bytes_from_stream;
                size -= bytes_from_stream;
            }
        }

        public static async Task EatStreamBytesAsync(this Stream stream, int size)
        {
            //See comment on THRASH_CAN and THRASH_CAN_SIZE.
            while (size > 0)
            {
                size -= (await stream.ReadAsync(THRASH_CAN, 0, size < THRASH_CAN_SIZE ? size : THRASH_CAN_SIZE));
            }
        }

        public static async Task<Stream> WriteInt32Async(this Stream stream, Int32 value)
        {
            await stream.WriteAsync(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value)), 0, 4);
            return stream;
        }

        public static async Task<Int32> ReadInt32Async(this Stream stream)
        {
            byte[] buffer = new byte[4];
            await stream.CheckedStreamReadAsync(buffer, 0, 4);
            return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, 0));
        }

        public static async Task<Stream> WriteInt16Async(this Stream stream, Int16 value)
        {
            await stream.WriteAsync(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value)), 0, 2);
            return stream;
        }

        public static async Task<Int16> ReadInt16Async(this Stream stream)
        {
            byte[] buffer = new byte[2];
            await stream.CheckedStreamReadAsync(buffer, 0, 2);
            return IPAddress.NetworkToHostOrder(BitConverter.ToInt16(buffer, 0));
        }
    }
}