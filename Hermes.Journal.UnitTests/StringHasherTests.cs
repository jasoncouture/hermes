using System.Text;
using Xunit;

namespace Hermes.Journal.UnitTests;

public class StringHasherTests
{
    [InlineData("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")]
    [InlineData("abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")]
    [Theory]
    public void ExpectedHashTests(string input, string expected)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        IStringHasher stringHasher = new StringHasher(new Sha256HashAlgorithm());

        var computedValue = stringHasher.ComputeHash(bytes);

        Assert.Equal(expected, computedValue);
    }
}