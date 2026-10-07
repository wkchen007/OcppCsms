using System.Collections.Concurrent;
using Microsoft.AspNetCore.Mvc;

namespace OcppCsms.Controllers;

[Route("connectors")]
public class ConnectorsController : ControllerBase
{
    private readonly ConcurrentDictionary<string, ConnectorState> _connectorStates;

    public ConnectorsController(ConcurrentDictionary<string, ConnectorState> connectorStates)
    {
        _connectorStates = connectorStates;
    }

    [HttpGet("{chargePointId}/{connectorId}")]
    public IActionResult GetConnector(string chargePointId, int connectorId)
    {
        var connectorKey = $"{chargePointId}/{connectorId}";

        if (_connectorStates.TryGetValue(connectorKey, out var state))
        {
            return Ok(state);
        }

        return NotFound();
    }
}
