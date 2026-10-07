using System.Collections.Concurrent;
using Microsoft.AspNetCore.Mvc;

namespace OcppCsms.Controllers;

[Route("transactions")]
public class TransactionsController : ControllerBase
{
    private readonly ConcurrentDictionary<int, Transaction> _transactions;

    public TransactionsController(ConcurrentDictionary<int, Transaction> transactions)
    {
        _transactions = transactions;
    }

    [HttpGet("{transactionId}")]
    public IActionResult GetTransaction(int transactionId)
    {
        if (_transactions.TryGetValue(transactionId, out var transaction))
        {
            return Ok(transaction);
        }

        return NotFound();
    }
}
