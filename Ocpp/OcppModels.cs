using System.Text.Json.Serialization;

#region BootNotification
public class BootNotificationReq // 6.3
{
    [JsonPropertyName("chargePointVendor")]
    public string? ChargePointVendor { get; set; }

    [JsonPropertyName("chargePointModel")]
    public string? ChargePointModel { get; set; }

    [JsonPropertyName("chargePointSerialNumber")]
    public string? ChargePointSerialNumber { get; set; }

    [JsonPropertyName("chargeBoxSerialNumber")]
    public string? ChargeBoxSerialNumber { get; set; }

    [JsonPropertyName("firmwareVersion")]
    public string? FirmwareVersion { get; set; }

    [JsonPropertyName("iccid")]
    public string? Iccid { get; set; }

    [JsonPropertyName("imsi")]
    public string? Imsi { get; set; }

    [JsonPropertyName("meterType")]
    public string? MeterType { get; set; }

    [JsonPropertyName("meterSerialNumber")]
    public string? MeterSerialNumber { get; set; }
}

public class BootNotificationConf // 6.4
{
    [JsonPropertyName("status")]
    public RegistrationStatus Status { get; set; }

    [JsonPropertyName("currentTime")]
    public string CurrentTime { get; set; } = "";

    [JsonPropertyName("interval")]
    public int Interval { get; set; }
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum RegistrationStatus // 7.38
{
    Accepted,
    Pending,
    Rejected
}
#endregion

#region Heartbeat
public class HeartbeatReq // 6.29
{
}

public class HeartbeatConf // 6.30
{
    [JsonPropertyName("currentTime")]
    public string CurrentTime { get; set; } = "";
}
#endregion

#region StatusNotification
public class StatusNotificationReq // 6.47
{
    [JsonPropertyName("connectorId")]
    public int? ConnectorId { get; set; }

    [JsonPropertyName("errorCode")]
    public ChargePointErrorCode? ErrorCode { get; set; }

    [JsonPropertyName("info")]
    public string? Info { get; set; }

    [JsonPropertyName("status")]
    public ChargePointStatus? Status { get; set; }

    [JsonPropertyName("timestamp")]
    public string? Timestamp { get; set; }

    [JsonPropertyName("vendorId")]
    public string? VendorId { get; set; }

    [JsonPropertyName("vendorErrorCode")]
    public string? VendorErrorCode { get; set; }
}

public class StatusNotificationConf // 6.48
{
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ChargePointErrorCode // 7.6
{
    ConnectorLockFailure,
    EVCommunicationError,
    GroundFailure,
    HighTemperature,
    InternalError,
    LocalListConflict,
    NoError,
    OtherError,
    OverCurrentFailure,
    OverVoltage,
    PowerMeterFailure,
    PowerSwitchFailure,
    ReaderFailure,
    ResetFailure,
    UnderVoltage,
    WeakSignal
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ChargePointStatus // 7.7
{
    Available,
    Preparing,
    Charging,
    SuspendedEVSE,
    SuspendedEV,
    Finishing,
    Reserved,
    Unavailable,
    Faulted
}
#endregion

public class ConnectorState
{
    public ChargePointStatus Status { get; set; }

    public ChargePointErrorCode ErrorCode { get; set; }

    public DateTime UpdatedAt { get; set; }
}

public class ChargePoint
{
    public string ChargePointId { get; set; } = "";
    public string Vendor { get; set; } = "";
    public string Model { get; set; } = "";
    public DateTime UpdatedAt { get; set; }
}

public class Transaction
{
    public int TransactionId { get; set; }

    public string ChargePointId { get; set; } = "";

    public int ConnectorId { get; set; }

    public string IdTag { get; set; } = "";

    public int MeterStart { get; set; }

    public DateTime StartedAt { get; set; }
}

public class TransactionIdGenerator
{
    private int _current = 0;

    public int Next()
    {
        return Interlocked.Increment(ref _current);
    }
}