using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql
{
    internal partial class NpgsqlBuffer
    {
        public async Task WriteAsync(byte[] buf, int offset, int count)
        {
            if (count <= WriteSpaceLeft)
            {
                Buffer.BlockCopy(buf, offset, _buf, _writePosition, count);
                _writePosition += count;
                return;
            }

            if (_writePosition != 0)
            {
                Buffer.BlockCopy(buf, offset, _buf, _writePosition, WriteSpaceLeft);
                offset += WriteSpaceLeft;
                count -= WriteSpaceLeft;
                await Underlying.WriteAsync(_buf, 0, Size);
                _writePosition = 0;
            }

            if (count >= Size)
            {
                await Underlying.WriteAsync(buf, offset, count);
            }
            else
            {
                Buffer.BlockCopy(buf, offset, _buf, 0, count);
                _writePosition = count;
            }
        }

        public async Task FlushAsync()
        {
            if (_writePosition != 0)
            {
                Contract.Assert(ReadBytesLeft == 0, "There cannot be read bytes buffered while a write operation is going on.");
                await Underlying.WriteAsync(_buf, 0, _writePosition);
                TotalBytesFlushed += _writePosition;
                _writePosition = 0;
            }
        }

        public async Task<NpgsqlBuffer> EnsureWriteAsync(int bytesToWrite)
        {
            Contract.Requires(bytesToWrite <= Size, "Requested write length larger than buffer size");
            if (bytesToWrite > WriteSpaceLeft)
            {
                await FlushAsync();
            }

            return this;
        }

        public async Task<NpgsqlBuffer> WriteBytesAsync(byte[] buf)
        {
            await WriteAsync(buf, 0, buf.Length);
            return this;
        }

        public async Task<NpgsqlBuffer> EnsuredWriteByteAsync(byte b)
        {
            if (WriteSpaceLeft == 0)
                await FlushAsync();
            _buf[_writePosition++] = b;
            return this;
        }

        public async Task<NpgsqlBuffer> EnsuredWriteInt32Async(int i)
        {
            if (WriteSpaceLeft < 4)
                await FlushAsync();
            var pos = _writePosition;
            _buf[pos++] = (byte)(i >> 24);
            _buf[pos++] = (byte)(i >> 16);
            _buf[pos++] = (byte)(i >> 8);
            _buf[pos++] = (byte)i;
            _writePosition = pos;
            return this;
        }

        public async Task<NpgsqlBuffer> EnsuredWriteInt16Async(int i)
        {
            if (WriteSpaceLeft < 2)
                await FlushAsync();
            _buf[_writePosition++] = (byte)(i >> 8);
            _buf[_writePosition++] = (byte)i;
            return this;
        }

        public async Task<NpgsqlBuffer> WriteStringAsync(string s, int byteLen)
        {
            Contract.Assume(TextEncoding == Encoding.UTF8, "WriteString assumes UTF8-encoding");
            int charPos = 0;
            for (;;)
            {
                if (byteLen <= WriteSpaceLeft)
                {
                    _writePosition += TextEncoding.GetBytes(s, charPos, s.Length - charPos, _buf, _writePosition);
                    return this;
                }

                int numCharsCanBeWritten = Math.Max(WriteSpaceLeft / 3, WriteSpaceLeft - (byteLen - (s.Length - charPos)));
                if (numCharsCanBeWritten >= 20) // Don't do this if the buffer is almost full
                {
                    char lastChar = s[charPos + numCharsCanBeWritten - 1];
                    if (lastChar >= 0xD800 && lastChar <= 0xDBFF)
                    {
                        --numCharsCanBeWritten; // Don't use high/lead surrogate pair as last char in block
                    }

                    int wrote = TextEncoding.GetBytes(s, charPos, numCharsCanBeWritten, _buf, _writePosition);
                    _writePosition += wrote;
                    byteLen -= wrote;
                    charPos += numCharsCanBeWritten;
                }
                else
                {
                    await Underlying.WriteAsync(_buf, 0, _writePosition);
                    _writePosition = 0;
                }
            }
        }

        public async Task<NpgsqlBuffer> WriteCharArrayAsync(char[] s, int byteLen)
        {
            Contract.Assume(TextEncoding == Encoding.UTF8, "WriteString assumes UTF8-encoding");
            int charPos = 0;
            for (;;)
            {
                if (byteLen <= WriteSpaceLeft)
                {
                    _writePosition += TextEncoding.GetBytes(s, charPos, s.Length - charPos, _buf, _writePosition);
                    return this;
                }

                int numCharsCanBeWritten = Math.Max(WriteSpaceLeft / 3, WriteSpaceLeft - (byteLen - (s.Length - charPos)));
                if (numCharsCanBeWritten >= 20) // Don't do this if the buffer is almost full
                {
                    char lastChar = s[charPos + numCharsCanBeWritten - 1];
                    if (lastChar >= 0xD800 && lastChar <= 0xDBFF)
                    {
                        --numCharsCanBeWritten; // Don't use high/lead surrogate pair as last char in block
                    }

                    int wrote = TextEncoding.GetBytes(s, charPos, numCharsCanBeWritten, _buf, _writePosition);
                    _writePosition += wrote;
                    byteLen -= wrote;
                    charPos += numCharsCanBeWritten;
                }
                else
                {
                    await Underlying.WriteAsync(_buf, 0, _writePosition);
                    _writePosition = 0;
                }
            }
        }
    }
}