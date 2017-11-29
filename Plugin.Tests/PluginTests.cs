using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk.Query;
using Cobalt.Components.CrmIQ.Plugin;
using Microsoft.Xrm.Sdk.Client;
using System.ServiceModel.Description;
using Microsoft.Xrm.Sdk;
using Microsoft.Crm.Sdk.Messages;

namespace Plugin.Tests
{
    [TestClass]
    public class PluginTests
    {
        [TestMethod]
        public void TestMarketingListNotInOneInAnother()
        {
            string fetchXml = @"
<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""true"">
  <entity name=""contact"">
    <attribute name=""fullname"" />
    <attribute name=""telephone1"" />
    <attribute name=""contactid"" />
    <order attribute=""fullname"" descending=""false"" />
    <link-entity name=""listmember"" from=""entityid"" to=""contactid"" visible=""false"" intersect=""true"">
      <link-entity name=""list"" from=""listid"" to=""listid"" alias=""ck"">
        <filter type=""and"">
          <condition attribute=""listid"" operator=""eq"" uiname=""Static Test List One"" uitype=""list"" value=""{CC4713BB-B294-E611-80D0-00155D030201}"" />
        </filter>
        <link-entity name=""listmember"" from=""listid"" to=""listid"" visible=""false"" intersect=""true"">
          <link-entity name=""contact"" from=""contactid"" to=""entityid"" alias=""cl"">
            <link-entity name=""listmember"" from=""entityid"" to=""contactid"" visible=""false"" intersect=""true"">
              <link-entity name=""list"" from=""listid"" to=""listid"" alias=""cm"">
                <filter type=""and"">
                  <condition attribute=""listid"" operator=""eq"" uiname=""Static Test List Two"" uitype=""list"" value=""{1E9B5CC1-B294-E611-80D0-00155D030201}"" />
                  <condition attribute=""listid"" operator=""null"" />
                </filter>
              </link-entity>
            </link-entity>
          </link-entity>
        </link-entity>
      </link-entity>
    </link-entity>
  </entity>
</fetch>
";
            TestFetchExpression(fetchXml, 1);
        }

        protected static void TestFetchExpression(string fetchXml, int expectedCount)
        {
            IOrganizationService service = GetOnPremiseService("crmdev@cobalt.net", "wezl.ch33zl", "cobalt.net", "http://cobalt3xdev16.cobalt.net", "IQDEV");
            FetchExpression expr = new FetchExpression(fetchXml);
            var conversionRequest = new FetchXmlToQueryExpressionRequest
            {
                FetchXml = fetchXml
            };

            FetchXmlToQueryExpressionResponse fetched = (FetchXmlToQueryExpressionResponse)service.Execute(conversionRequest);
            fetched.Query.NoLock = true;
            QueryExpression oneOffQuery = fetched.Query;

            PluginAdapter adapter = new PluginAdapter(string.Empty, string.Empty);
            oneOffQuery = adapter.UpdateQuery(new MetadataService(service), (OrganizationServiceProxy)service, null, oneOffQuery);

            var backToFetchRequest = new QueryExpressionToFetchXmlRequest
            {
                Query = oneOffQuery
            };
            QueryExpressionToFetchXmlResponse queried = (QueryExpressionToFetchXmlResponse)service.Execute(backToFetchRequest);
            string updatedFetch = queried.FetchXml;

            EntityCollection collection = service.RetrieveMultiple(oneOffQuery);
            Assert.AreEqual(collection.Entities.Count, expectedCount);
        }

        protected static IOrganizationService GetOnPremiseService(string UserName, string Password, string Domian, string URL, string OrgName)
        {
            IOrganizationService service = null;
            try
            {
                ClientCredentials cred = new ClientCredentials();
                cred.Windows.ClientCredential = new System.Net.NetworkCredential(UserName, Password, Domian);
                OrganizationServiceProxy proxy = new OrganizationServiceProxy(new Uri(URL + "/" + OrgName + "/XRMServices/2011/Organization.svc"), null, cred, null);
                service = (IOrganizationService)proxy;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return service;
        }
    }
}
