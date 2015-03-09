using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Common.Logging;
using Mono.Security.Protocol.Tls;
using Npgsql.Localization;
using Npgsql.Messages;
using Npgsql.TypeHandlers;
using NpgsqlTypes;
using System.Text;
using Npgsql.FrontendMessages;
using SecurityProtocolType = Mono.Security.Protocol.Tls.SecurityProtocolType;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql
{
    /// <summary>
        /// Represents a connection to a PostgreSQL backend. Unlike NpgsqlConnection objects, which are
        /// exposed to users, connectors are internal to Npgsql and are recycled by the connection pool.
        /// </summary>
    internal partial class NpgsqlConnector
    {
        internal async Task SendAllMessagesAsync()
        {
            try
            {
                foreach (var msg in _messagesToSend)
                {
                    await SendMessageAsync(msg);
                }

                await Buffer.FlushAsync();
            }
            finally
            {
                _messagesToSend.Clear();
            }
        }

        internal async Task SendMessageAsync(FrontendMessage msg)
        {
            _log.DebugFormat("Sending: {0}", msg);
            var asSimple = msg as SimpleFrontendMessage;
            if (asSimple != null)
            {
                if (asSimple.Length > Buffer.WriteSpaceLeft)
                {
                    await Buffer.FlushAsync();
                }

                Contract.Assume(Buffer.WriteSpaceLeft >= asSimple.Length);
                asSimple.Write(Buffer);
                return;
            }

            var asComplex = msg as ComplexFrontendMessage;
            if (asComplex != null)
            {
                byte[] directBuf;
                while (!asComplex.Write(Buffer, out directBuf))
                {
                    await Buffer.FlushAsync();
                    // The following is an optimization hack for writing large byte arrays without passing
                    // through our buffer
                    if (directBuf != null)
                    {
                        await Buffer.Underlying.WriteAsync(directBuf, 0, directBuf.Length);
                    }
                }

                return;
            }

            throw PGUtil.ThrowIfReached();
        }

        internal async Task SendQueryAsync(string query)
        {
            _log.DebugFormat("Sending query: {0}", query);
            QueryManager.WriteQuery(Buffer, query);
            await Buffer.FlushAsync();
            State = ConnectorState.Executing;
        }

        internal async Task SendQueryAsync(byte[] query)
        {
            _log.Debug(m => m("Sending query: {0}", Encoding.UTF8.GetString(query, 0, query.Length - 1)));
            QueryManager.WriteQuery(Buffer, query);
            await Buffer.FlushAsync();
            State = ConnectorState.Executing;
        }

        internal async Task SendQueryRawAsync(byte[] rawQuery)
        {
            _log.Debug("Sending raw query");
            await Buffer.WriteAsync(rawQuery, 0, rawQuery.Length);
            await Buffer.FlushAsync();
            State = ConnectorState.Executing;
        }

        internal async Task<NotificationBlock> BlockNotificationsAsync()
        {
            if (_notificationSemaphore != null)
            {
                var n = new NotificationBlock(this);
                if (++_notificationBlockRecursionDepth == 1)
                    await _notificationSemaphore.WaitAsync();
                return n;
            }
            else
            {
                return null;
            }
        }

        internal async Task ExecuteBlindAsync(string query)
        {
            using (await BlockNotificationsAsync())
            {
                await SetBackendCommandTimeoutAsync(20);
                await SendQueryAsync(query);
                SkipUntil(BackendMessageCode.ReadyForQuery);
                State = ConnectorState.Ready;
            }
        }

        internal async Task ExecuteBlindAsync(byte[] query)
        {
            using (await BlockNotificationsAsync())
            {
                await SetBackendCommandTimeoutAsync(20);
                await SendQueryRawAsync(query);
                SkipUntil(BackendMessageCode.ReadyForQuery);
                State = ConnectorState.Ready;
            }
        }

        internal async Task ExecuteBlindSuppressTimeoutAsync(string query)
        {
            using (await BlockNotificationsAsync())
            {
                await SendQueryAsync(query);
                SkipUntil(BackendMessageCode.ReadyForQuery);
                State = ConnectorState.Ready;
            }
        }

        internal async Task ExecuteBlindSuppressTimeoutAsync(byte[] query)
        {
            // Block the notification thread before writing anything to the wire.
            using (await BlockNotificationsAsync())
            {
                await SendQueryRawAsync(query);
                SkipUntil(BackendMessageCode.ReadyForQuery);
                State = ConnectorState.Ready;
            }
        }

        internal async Task ExecuteSetStatementTimeoutBlindAsync(int timeout)
        {
            // Optimize for a few common timeout values.
            switch (timeout)
            {
                case 10:
                    SendQueryRaw(QueryManager.SetStmtTimeout10Sec);
                    break;
                case 20:
                    SendQueryRaw(QueryManager.SetStmtTimeout20Sec);
                    break;
                case 30:
                    SendQueryRaw(QueryManager.SetStmtTimeout30Sec);
                    break;
                case 60:
                    SendQueryRaw(QueryManager.SetStmtTimeout60Sec);
                    break;
                case 90:
                    SendQueryRaw(QueryManager.SetStmtTimeout90Sec);
                    break;
                case 120:
                    SendQueryRaw(QueryManager.SetStmtTimeout120Sec);
                    break;
                default:
                    await SendQueryAsync(string.Format("SET statement_timeout = {0}", timeout * 1000));
                    break;
            }

            SkipUntil(BackendMessageCode.ReadyForQuery);
            State = ConnectorState.Ready;
        }

        internal async Task SetBackendCommandTimeoutAsync(int timeout)
        {
            if (Mediator.BackendCommandTimeout == -1 || Mediator.BackendCommandTimeout != timeout)
            {
                await ExecuteSetStatementTimeoutBlindAsync(timeout);
                Mediator.BackendCommandTimeout = timeout;
            }
        }
    }
}