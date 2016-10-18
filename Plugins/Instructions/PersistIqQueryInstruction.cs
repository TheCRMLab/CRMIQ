using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    [InstructionName("PersistIqQuery")]
    public class PersistIqQueryInstruction : Instruction
    {
        //System and User query object type codes
        //9602 - Goal Rollup Query, maybe needed to be added later
        private static readonly List<int> eligibleQueryObjectTypeCodes = new List<int> { 1039, 4230 };

        private static readonly List<string> eligibleSteps = new List<string>() { "Create", "Update" };

        public PersistIqQueryInstruction()
        {
            this.ObjectTypeCodes = new List<int>();
        }

        [DataMember(EmitDefaultValue = false)]
        public List<int> ObjectTypeCodes { get; set; }

        public override string Execute()
        {
            EntityCollection collection = RetrieveSdkMessageProcessingStepsForPersistingIq();

            List<Tuple<int,string>> queriedEntities = new List<Tuple<int, string>>();

            if (this.ObjectTypeCodes == null)
            {
                this.ObjectTypeCodes = new List<int>();
            }

            //Get each sdk message processing step for both create and update
            foreach (Entity processingStep in collection.Entities)
            {
                if (processingStep.Attributes["a1.primaryobjecttypecode"] != null)
                { 
                    string entity = (processingStep.Attributes["a1.primaryobjecttypecode"] as AliasedValue).Value.ToString();
                    EntityMetadata metadata = this.MetaDataService.RetrieveMetadata(entity);

                    if (metadata != null && metadata.ObjectTypeCode != null && metadata.ObjectTypeCode.HasValue)
                    {
                        queriedEntities.Add(new Tuple<int, string>(metadata.ObjectTypeCode.Value, processingStep.Attributes["sdkmessage.name"].ToString()));
                    }

                    if (!this.ObjectTypeCodes.Contains(metadata.ObjectTypeCode.Value) || !eligibleQueryObjectTypeCodes.Contains(metadata.ObjectTypeCode.Value))
                    {
                        Service.Delete(processingStep.LogicalName, processingStep.Id);
                    }
                }
            }

            Entity pluginType = this.RetrievePluginType();
            //Create steps for object type codes that do not have one already.
            foreach (int objectTypeCode in this.ObjectTypeCodes)
            {
                foreach(string stepName in eligibleSteps)
                {
                    if(!queriedEntities.Contains(new Tuple<int, string>(objectTypeCode, stepName)))
                    {
                        EntityCollection filters = this.RetrieveSdkMessageFilters(stepName);
                        Entity message = this.RetrieveMessage(stepName);
                        EntityMetadata entityMetadata = this.MetaDataService.RetrieveMetadataByObjectTypeCode(objectTypeCode);

                        if (entityMetadata != null)
                        {
                            Entity step = new Entity("sdkmessageprocessingstep");
                            step.Attributes["eventhandler"] = new EntityReference(pluginType.LogicalName, pluginType.Id);
                            step.Attributes["name"] = String.Format("Cobalt.Components.CrmIQ.Plugin.PluginAdapter: {0} of {1}", stepName, entityMetadata.LogicalName);
                            step.Attributes["description"] = String.Format("Cobalt.Components.CrmIQ.Plugin.PluginAdapter: {0} of {1}", stepName, entityMetadata.LogicalName);
                            step.Attributes["mode"] = new OptionSetValue(0);
                            step.Attributes["sdkmessageid"] = new EntityReference(message.LogicalName, message.Id);
                            step.Attributes["supporteddeployment"] = new OptionSetValue(0);
                            step.Attributes["rank"] = 1;
                            step.Attributes["filteringattributes"] = String.Empty;
                            step.Attributes["stage"] = new OptionSetValue(20);

                            Entity filter = filters.Entities.FirstOrDefault(e => e.Attributes.ContainsKey("primaryobjecttypecode") && e.Attributes["primaryobjecttypecode"].ToString() == entityMetadata.LogicalName);
                            if (filter != null)
                            {
                                step.Attributes["sdkmessagefilterid"] = new EntityReference(filter.LogicalName, filter.Id);
                            }

                            Service.Create(step);
                        }
                    }
                }
            }

            return (new RetrieveExistingPersistIqQueryPluginsInstruction() { Service = this.Service }).Execute();
        }

        protected virtual Entity RetrieveMessage(string messageName)
        {
            return this.ExecuteFetchScalar(
                string.Format(@"<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"">
                  <entity name=""sdkmessage"">
                    <attribute name=""name"" />
                    <filter type=""and"">
                      <condition attribute=""name"" operator=""eq"" value=""{0}"" />
                    </filter>
                  </entity>
                </fetch>", messageName));
        }

        protected virtual Entity RetrievePluginType()
        {
            return this.ExecuteFetchScalar(
                @"<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"">
                    <entity name=""plugintype"">
                    <attribute name=""plugintypeid"" />
                    <order attribute=""friendlyname"" descending=""false"" />
                    <filter type=""and"">
                        <condition attribute=""typename"" operator=""eq"" value=""Cobalt.Components.CrmIQ.Plugin.PluginAdapter"" />
                    </filter>
                    </entity>
                </fetch>");
        }

        protected EntityCollection RetrieveSdkMessageFilters(string messageName)
        {
            return this.ExecuteFetchQuery(
            string.Format(@"<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"">
              <entity name=""sdkmessagefilter"">
                <attribute name=""sdkmessageid"" />
                <attribute name=""primaryobjecttypecode"" />
                <attribute name=""secondaryobjecttypecode"" />
                <attribute name=""availability"" />
                <attribute name=""iscustomprocessingstepallowed"" />
                <link-entity name=""sdkmessage"" from=""sdkmessageid"" to=""sdkmessageid"" alias=""ab"">
                  <filter type=""and"">
                    <condition attribute=""name"" operator=""eq"" value=""{0}"" />
                  </filter>
                </link-entity>
              </entity>
            </fetch>", messageName));
        }

    }
}
