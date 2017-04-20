using NUnit.Framework;
using OdpNetMicroMapper;

namespace Test
{
    class CammelCaseTest
    {
        [Test]
        public void CammelCaseRules()
        {
            Assert.AreEqual("ID".CammelCase(), "Id");
            Assert.AreEqual("FUND".CammelCase(), "Fund");
            Assert.AreEqual("id".CammelCase(), "Id");
            Assert.AreEqual("fund_id".CammelCase(), "FundId");
            Assert.AreEqual("yield_2date".CammelCase(), "Yield_2date");
            Assert.AreEqual("_test".CammelCase(), "_test");
            Assert.AreEqual("test_".CammelCase(), "Test_");
        }

        [TestCase("id")]
        [TestCase("fund_id")]
        [TestCase("yield_2date")]
        [TestCase("yield__2date")]
        [TestCase("yield_2_date")]
        [TestCase("test")]
        [TestCase("test_")]
        [TestCase("_test")]
        [TestCase("_test_")]
        [TestCase("1_2_3_name")]
        [TestCase("_")]
        public void IsReverseableTest(string column)
        {
            Assert.AreEqual(column, column.CammelCase().UnCammelCase(), "CammelCase is " + column.CammelCase());
        }
    }
}
