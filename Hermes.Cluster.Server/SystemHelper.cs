using System.Collections.Immutable;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Microsoft.AspNetCore.HttpOverrides;

public static class SystemHelper
{
    public const int ClusterPort = 9090;

    public static IReadOnlyCollection<IPNetwork> DetermineIpNetworks(string[] cidrNetworks)
    {
        return GetIpNetworks(cidrNetworks) ?? GetIpNetworkFromHostInterfaces();
    }

    public static IEnumerable<UnicastIPAddressInformation> GetHostNetworkAddressInformation(IPNetwork network)
    {
        return NetworkInterface.GetAllNetworkInterfaces()
            .Where(networkInterface => networkInterface.OperationalStatus == OperationalStatus.Up ||
                                       networkInterface.OperationalStatus == OperationalStatus.Unknown)
            .SelectMany(networkInterface => networkInterface.GetIPProperties().UnicastAddresses)
            .Where(i => network.Contains(i.Address));
    }

    private static IReadOnlyCollection<IPNetwork> GetIpNetworkFromHostInterfaces()
    {
        List<IPNetwork> networks = new List<IPNetwork>();
        List<IPNetwork> bannedNetworks = new List<IPNetwork>
        {
            new(IPAddress.Loopback, 8),
            new(IPAddress.IPv6Loopback, 128),
            new(IPAddress.Broadcast, 32),
            new(IPAddress.None, 32),
            new(IPAddress.Broadcast, 32),
            new(IPAddress.Any, 32),
            new(IPAddress.IPv6Any, 128)
        };
        foreach (var unicastNetwork in GetHostNetworkAddressInformation(new IPNetwork(IPAddress.Any, 0)))
        {
            if (bannedNetworks.Any(i => i.Contains(unicastNetwork.Address))) continue;
            try
            {
                networks.Add(new IPNetwork(unicastNetwork.Address, unicastNetwork.PrefixLength));
            }
            catch
            {
                // Prefix length must not be supported. Skip it.
            }
        }

        if (networks.Count == 0) throw new InvalidOperationException("Unable to determine local host subnets!");
        return networks.AsReadOnly();
    }

    public static IReadOnlyCollection<IPNetwork>? GetIpNetworks(params string[] cidrNetworks)
    {
        cidrNetworks = cidrNetworks.Where(i => !string.IsNullOrWhiteSpace(i)).ToArray();
        if (cidrNetworks.Length < 1) return null;

        IPNetwork ParseNetwork(string cidrNetwork)
        {
            if (string.IsNullOrWhiteSpace(cidrNetwork))
                return null;
            var parts = cidrNetwork.Split('/');
            var networkAddress = IPAddress.Parse(parts[0]);
            int prefixLength = networkAddress.AddressFamily == AddressFamily.InterNetwork ? 32 : 128;
            if (parts.Length == 2)
            {
                prefixLength = int.Parse(parts[1]);
            }

            return new IPNetwork(networkAddress, prefixLength);
        }

        List<IPNetwork> returnValue = new List<IPNetwork>();
        // if we have multiple definitions of the same network, pick the network with the smallest prefix length
        // AKA: The largest network.
        foreach (var networkGroup in cidrNetworks.Select(ParseNetwork).ToLookup(i => i.Prefix))
        {
            var network = networkGroup.OrderBy(i => i.PrefixLength).First();
            returnValue.Add(network);
        }

        return returnValue.ToImmutableList();
    }
}