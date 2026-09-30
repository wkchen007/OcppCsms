enum ConnectorStatus
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

enum ChargePointErrorCode
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

class ConnectorState
{
    public ConnectorStatus Status { get; set; }

    public ChargePointErrorCode ErrorCode { get; set; }

    public DateTime UpdatedAt { get; set; }
}

class Transaction
{
    public int TransactionId { get; set; }

    public string ChargePointId { get; set; } = "";

    public int ConnectorId { get; set; }

    public string IdTag { get; set; } = "";

    public int MeterStart { get; set; }

    public DateTime StartedAt { get; set; }
}

class TransactionIdGenerator
{
    private int _current = 0;

    public int Next()
    {
        return Interlocked.Increment(ref _current);
    }
}
