using System;
using Npgsql.Messages;
using NpgsqlTypes;
using System.Data;
using System.Diagnostics.Contracts;

namespace Npgsql.TypeHandlers.DateTimeHandlers
{
    /// <remarks>
    /// http://www.postgresql.org/docs/9.3/static/datatype-datetime.html
    /// </remarks>
    [TypeMapping("timestamp", NpgsqlDbType.Timestamp, new[] { DbType.DateTime, DbType.DateTime2 }, typeof(NpgsqlTimeStamp))]
    internal class TimeStampHandler : TypeHandlerWithPsv<DateTime, NpgsqlTimeStamp>, ITypeHandler<NpgsqlTimeStamp>
    {
        public override bool SupportsBinaryWrite
        {
            get
            {
                return true;
            }
        }

        public override DateTime Read(NpgsqlBuffer buf, FieldDescription fieldDescription, int len)
        {
            // TODO: Convert directly to DateTime without passing through NpgsqlTimeStamp?
            return (DateTime)((ITypeHandler<NpgsqlTimeStamp>)this).Read(buf, fieldDescription, len);
        }

        NpgsqlTimeStamp ITypeHandler<NpgsqlTimeStamp>.Read(NpgsqlBuffer buf, FieldDescription fieldDescription, int len)
        {
            switch (fieldDescription.FormatCode)
            {
                case FormatCode.Text:
                    return NpgsqlTimeStamp.Parse(buf.ReadString(len));
                case FormatCode.Binary:
                    return NpgsqlTimeStamp.FromInt64(buf.ReadInt64());
                default:
                    throw PGUtil.ThrowIfReached("Unknown format code: " + fieldDescription.FormatCode);
            }
        }

        internal override int Length(object value)
        {
            return 8;
        }

        internal override void WriteBinary(object value, NpgsqlBuffer buf)
        {
            NpgsqlTimeStamp timestamp = new NpgsqlTimeStamp();

            if ( value is DateTime )
            {
                var dtValue = (DateTime)value;
                var datePart = new NpgsqlDate(dtValue);
                var timePart = new NpgsqlTime(dtValue.Hour, dtValue.Minute, dtValue.Second, dtValue.Millisecond * 1000);
                timestamp = new NpgsqlTimeStamp(datePart, timePart);
            }
            else if ( value is string )
            {
                timestamp = NpgsqlTimeStamp.Parse((string)value);
            }

            var uSecsTime = timestamp.Time.Hours * 3600000000L + timestamp.Time.Minutes * 60000000L + timestamp.Time.Seconds * 1000000L + timestamp.Time.Microseconds;

            if ( timestamp >= new NpgsqlTimeStamp(2000, 1, 1, 0, 0, 0) )
            {
                var uSecsDate = ( timestamp.Date.DaysSinceEra - 730119 ) * 86400000000L;
                buf.WriteInt64(uSecsDate + uSecsTime);
            }
            else
            {
                var uSecsDate = ( 730119 - timestamp.Date.DaysSinceEra ) * 86400000000L;
                buf.WriteInt64(-( uSecsDate - uSecsTime ));
            }
        }
    }
}
