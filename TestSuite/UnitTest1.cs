namespace TestSuite;

[TestClass]
public class BasicParsingTests
{
    public class Entry
    {
        public Int32 No { get; set; }
        public String Name { get; set; }
    }


    [TestMethod]
    public void TestMethod1()
    {
        TestPipeEnd(PipeEnds.FromFile(@"files/input.xml").Xml<Entry>());
    }

    static String[] expectedNames = new[] { "foo", "bar", "baz" };

    void TestPipeEnd(IEnumerablePipeEnd<Entry> pipeEnd)
    {
        var items = pipeEnd.ReadAll();

        Assert.AreEqual(items.Length, 3);

        for (var i = 0; i < items.Length; i++)
        {
            Assert.AreEqual(i + 1, items[i].No);
            Assert.AreEqual(expectedNames[i], items[i].Name);
        }
    }
}