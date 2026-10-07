using System.Collections.Concurrent;
using Microsoft.AspNetCore.Mvc;

namespace OcppCsms.Controllers;

[Route("chargepoints")]
public class ChargePointsController : ControllerBase
{
    private readonly ConcurrentDictionary<string, ConnectorState> _connectorStates;
    private readonly ConcurrentDictionary<string, bool> _chargePointConnections;
    private readonly ConcurrentDictionary<string, ChargePoint> _chargePoints;

    public ChargePointsController(
        ConcurrentDictionary<string, ConnectorState> connectorStates,
        ConcurrentDictionary<string, bool> chargePointConnections,
        ConcurrentDictionary<string, ChargePoint> chargePoints)
    {
        _connectorStates = connectorStates;
        _chargePointConnections = chargePointConnections;
        _chargePoints = chargePoints;
    }

    [HttpGet("{chargePointId}/connectors")]
    public IActionResult GetConnectors(string chargePointId)
    {
        var prefix = $"{chargePointId}/";

        var connectors = _connectorStates
            .Where(x => x.Key.StartsWith(prefix))
            .Select(x => new
            {
                ConnectorId = int.Parse(x.Key.Split('/')[1]),
                Status = x.Value.Status,
                ErrorCode = x.Value.ErrorCode,
                UpdatedAt = x.Value.UpdatedAt
            })
            .OrderBy(x => x.ConnectorId)
            .ToList();

        return Ok(connectors);
    }

    [HttpGet("{chargePointId}")]
    public IActionResult GetChargePoint(string chargePointId)
    {
        if (!_chargePointConnections.TryGetValue(chargePointId, out var connected))
        {
            return NotFound();
        }

        _chargePoints.TryGetValue(chargePointId, out var chargePoint);

        return Ok(new
        {
            ChargePointId = chargePointId,
            Connected = connected,
            Vendor = chargePoint?.Vendor,
            Model = chargePoint?.Model,
            UpdatedAt = chargePoint?.UpdatedAt
        });
    }
}
