using System;
using System.Runtime.Serialization;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    public abstract class Instruction
    {
        public abstract string Execute();
        private IOrganizationService service;
        public IOrganizationService Service
        {
            get
            {
                return this.service;
            }
            set
            {
                this.MetaDataService = new MetadataService(value);
                this.service = value;
            }
        }

        protected MetadataService MetaDataService;

        protected Entity ExecuteFetchScalar(string fetchXml)
        {
            Entity returnValue = null;
            EntityCollection collection = this.ExecuteFetchQuery(fetchXml);
            if (collection != null && collection.Entities != null && collection.Entities.Count > 0)
            {
                returnValue = collection.Entities[0];
            }
            return returnValue;
        }
        protected EntityCollection ExecuteFetchQuery(string fetchXml)
        {
            var conversionRequest = new FetchXmlToQueryExpressionRequest() { FetchXml = fetchXml };
            FetchXmlToQueryExpressionResponse fetched = (FetchXmlToQueryExpressionResponse)this.Service.Execute(conversionRequest);
            fetched.Query.NoLock = true;
            QueryExpression pluginQuery = fetched.Query;

            return this.Service.RetrieveMultiple(pluginQuery);
        }
        protected EntityCollection RetrieveSdkMessageProcessingSteps()
        {
            return this.ExecuteFetchQuery(
                @"<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"">
                  <entity name=""sdkmessageprocessingstep"">
                    <attribute name=""name"" />
                    <attribute name=""description"" />
                    <attribute name=""eventhandler"" />
                    <attribute name=""impersonatinguserid"" />
                    <attribute name=""supporteddeployment"" />
                    <attribute name=""statuscode"" />
                    <attribute name=""statecode"" />
                    <attribute name=""sdkmessagefilterid"" />
                    <attribute name=""sdkmessageid"" />
                    <attribute name=""filteringattributes"" />
                    <attribute name=""configuration"" />
                    <attribute name=""rank"" />
                    <attribute name=""mode"" />
                    <attribute name=""stage"" />
                    <attribute name=""asyncautodelete"" />
                    <link-entity name=""sdkmessagefilter"" from=""sdkmessagefilterid"" to=""sdkmessagefilterid"" visible=""false"" link-type=""outer"" alias=""a1"">
                      <attribute name=""secondaryobjecttypecode"" />
                      <attribute name=""primaryobjecttypecode"" />
                    </link-entity>
                    <link-entity name=""plugintype"" from=""plugintypeid"" to=""plugintypeid"" alias=""an"">
                      <link-entity name=""pluginassembly"" from=""pluginassemblyid"" to=""pluginassemblyid"" alias=""ao"">
                        <filter type=""and"">
                          <condition attribute=""name"" operator=""eq"" value=""Cobalt.Components.CrmIQ.Plugins"" />
                        </filter>
                      </link-entity>
                    </link-entity>
                    <link-entity name=""sdkmessage"" from=""sdkmessageid"" to=""sdkmessageid"" alias=""ap"">
                      <filter type=""and"">
                        <condition attribute=""name"" operator=""eq"" value=""RetrieveMultiple"" />
                      </filter>
                    </link-entity>
                  </entity>
                </fetch>");
        }

        protected EntityCollection RetrieveSdkMessageProcessingStepsForPersistingIq()
        {
            return this.ExecuteFetchQuery(
                @"<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"">
                      <entity name=""sdkmessageprocessingstep"">
                        <attribute name=""name"" />
                        <attribute name=""description"" />
                        <attribute name=""eventhandler"" />
                        <attribute name=""impersonatinguserid"" />
                        <attribute name=""supporteddeployment"" />
                        <attribute name=""statuscode"" />
                        <attribute name=""statecode"" />
                        <attribute name=""sdkmessagefilterid"" />
                        <attribute name=""sdkmessageid"" />
                        <attribute name=""filteringattributes"" />
                        <attribute name=""configuration"" />
                        <attribute name=""asyncautodelete"" />
                        <link-entity name=""sdkmessagefilter"" from=""sdkmessagefilterid"" to=""sdkmessagefilterid"" visible=""false"" link-type=""outer"" alias=""a1"">
                          <attribute name=""secondaryobjecttypecode"" />
                          <attribute name=""primaryobjecttypecode"" />
                        </link-entity>
                        <link-entity name=""plugintype"" from=""plugintypeid"" to=""plugintypeid"" alias=""al"">
                          <link-entity name=""pluginassembly"" from=""pluginassemblyid"" to=""pluginassemblyid"" alias=""am"">
                            <filter type=""and"">
                              <condition attribute=""name"" operator=""eq"" value=""Cobalt.Components.CrmIQ.Plugins"" />
                            </filter>
                          </link-entity>
                        </link-entity>
                        <link-entity name=""sdkmessage"" from=""sdkmessageid"" to=""sdkmessageid"" alias=""sdkmessage"">
                          <attribute name=""name"" />
                          <filter type=""and"">
                            <filter type=""or"">
                              <condition attribute=""name"" operator=""eq"" value=""Create"" />
                              <condition attribute=""name"" operator=""eq"" value=""Update"" />
                            </filter>
                          </filter>
                        </link-entity>
                      </entity>
                    </fetch>");
        }
    }
}
