using System.Net;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft.Http;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using DotNext.Net.Http;
using Microsoft.AspNetCore.Mvc;

namespace Hermes.Journal.Server.Controllers;

[ApiController]
[Route("api/clusterState")]
public class ClusterStateController : Controller
{
    private readonly IRaftHttpCluster _cluster;
    private readonly IClusterConfigurationStorage<HttpEndPoint> _configurationStorage;

    public ClusterStateController(IRaftHttpCluster cluster, IClusterConfigurationStorage<HttpEndPoint> configurationStorage)
    {
        _cluster = cluster;
        _configurationStorage = configurationStorage;
    }
    [HttpGet("identity")]
    public IActionResult Identity()
    {
        return Ok(new
        {
            Name = Environment.MachineName,
            _cluster.LocalMemberAddress.Host,
            _cluster.LocalMemberAddress.Port,
            _cluster.LocalMemberAddress.IsSecure,
            Id = _cluster.LocalMemberId
        });
    }
    [HttpPost("member/add")]
    public async Task<IActionResult> AddMember(string host, int port, bool isSecure = false, CancellationToken cancellationToken = default)
    {
        var member = new HttpEndPoint(host, port, isSecure);
        var memberId = ClusterMemberId.FromEndPoint(member);
        if (await _cluster.AddMemberAsync(memberId, member, cancellationToken).ConfigureAwait(false))
        {
            return StatusCode((int)HttpStatusCode.NoContent);
        }

        return StatusCode((int)HttpStatusCode.ServiceUnavailable);
    }

    [HttpGet("members")]
    public IActionResult GetMembers()
    {
        return Ok(_configurationStorage.ActiveConfiguration.Values.ToList().Select(i => i.ToString()).ToArray());
    }

    [HttpGet("leader")]
    public IActionResult Leader()
    {
        if (_cluster.Readiness.IsCompleted)
        {
            var leader = _cluster.Leader?.EndPoint as HttpEndPoint;
            if (leader == null) return StatusCode((int) HttpStatusCode.ServiceUnavailable);
            return Ok(new LeaderDataModel(leader.Host, leader.Port, leader.IsSecure));
        }

        return StatusCode((int) HttpStatusCode.ServiceUnavailable);
    }
}

public record LeaderDataModel(string Host, int Port, bool IsSecure);