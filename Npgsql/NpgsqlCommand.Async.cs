using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Npgsql.Localization;
using Npgsql.Messages;
using NpgsqlTypes;
using System.Diagnostics.Contracts;
using Npgsql.FrontendMessages;

namespace Npgsql
{
    public sealed partial class NpgsqlCommand
    {
        async Task<int> ExecuteNonQueryInternalAsync()
        {
            _log.Debug("ExecuteNonQuery");
            NpgsqlDataReader reader;
            using (reader = (await GetReaderAsync()))
            {
                while (reader.NextResult())
                    ;
            }

            return reader.RecordsAffected;
        }

        async Task<object> ExecuteScalarInternalAsync()
        {
            using (var reader = (await GetReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleRow)))
            {
                return reader.Read() && reader.FieldCount != 0 ? reader.GetValue(0) : null;
            }
        }

        async Task<NpgsqlDataReader> ExecuteDbDataReaderInternalAsync(CommandBehavior behavior)
        {
            // Close connection if requested even when there is an error.
            try
            {
                return await GetReaderAsync(behavior);
            }
            catch (Exception)
            {
                if ((behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
                {
                    _connection.Close();
                }

                throw;
            }
        }

        internal async Task<NpgsqlDataReader> GetReaderAsync(CommandBehavior behavior = CommandBehavior.Default)
        {
            CheckConnectionState();
            // TODO: Actual checks...
            Contract.Assert(_connector.Buffer.ReadBytesLeft == 0, "The read buffer should be read completely before sending Parse message");
            Contract.Assert(_connector.Buffer.WritePosition == 0, "WritePosition should be 0");
            NpgsqlDataReader reader = null;
            switch (_prepared)
            {
                case PrepareStatus.NotPrepared:
                    AddParseAndDescribeMessages();
                    break;
                case PrepareStatus.NeedsPrepare:
                    Prepare();
                    goto case PrepareStatus.Prepared;
                case PrepareStatus.Prepared:
                    break;
                default:
                    throw PGUtil.ThrowIfReached();
            }

            if ((behavior & CommandBehavior.SchemaOnly) == 0)
            {
                // In SchemaOnly mode we skip over Bind and Execute
                var bindMessage = new BindMessage(_connector.TypeHandlerRegistry, _parameters.Where(p => p.IsInputDirection).ToList(), "", IsPrepared ? _planName : "");
                bindMessage.Prepare();
                _connector.AddMessage(bindMessage);
                _connector.AddMessage(new ExecuteMessage("", (behavior & CommandBehavior.SingleRow) != 0 ? 1 : 0));
            }
            else
            {
                if (IsPrepared)
                {
                    throw new NotImplementedException("Prepared SchemaOnly not implemented yet");
                }
            }

            _connector.AddMessage(SyncMessage.Instance);
            // Block the notification thread before writing anything to the wire.
            _notificationBlock = (await _connector.BlockNotificationsAsync());
            //using (_connector.BlockNotificationThread())
            try
            {
                State = CommandState.InProgress;
                await // TODO: Can this be combined into the message chain?
_connector.SetBackendCommandTimeoutAsync(CommandTimeout);
                await _connector.SendAllMessagesAsync();
                BackendMessage msg;
                do
                {
                    msg = _connector.ReadSingleMessage();
                }
                while (!ProcessMessage(msg, behavior));
                return new NpgsqlDataReader(this, behavior, _rowDescription);
            }
            catch (NpgsqlException)
            {
                // TODO: Should probably happen inside ReadSingleMessage()
                _connector.State = ConnectorState.Ready;
                throw;
            }
            finally
            {
                if (reader == null && _notificationBlock != null)
                {
                    _notificationBlock.Dispose();
                }
            }
        }
    }
}