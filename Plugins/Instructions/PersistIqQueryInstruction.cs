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
        private static readonly List<string> eligibleQueryEntities = new List<string> { "savedquery", "userquery" };

        private static readonly List<string> eligibleSteps = new List<string>() { "Create", "Update" };

        public PersistIqQueryInstruction()
        {
            this.Entities = new List<string>();
        }

        [DataMember(EmitDefaultValue = false)]
        public List<string> Entities { get; set; }

        public override string Execute()
        {
            EntityCollection collection = RetrieveSdkMessageProcessingStepsForPersistingIq();

            List<Tuple<string,string>> queriedEntities = new List<Tuple<string, string>>();

            if (this.Entities == null)
            {
                this.Entities = new List<string>();
            }
            //Get each sdk message processing step for both create and update
            foreach (Entity processingStep in collection.Entities)
            {
                if (processingStep.Attributes["a1.primaryobjecttypecode"] != null)
                {
                    string entityName = (processingStep.Attributes["a1.primaryobjecttypecode"] as AliasedValue).Value.ToString();
                    EntityMetadata metadata = this.MetaDataService.RetrieveMetadata(entityName);

                    if (metadata != null)
                    {
                        queriedEntities.Add(new Tuple<string, string>(metadata.LogicalName, processingStep.Attributes["sdkmessage.name"].ToString()));
                    }
                    if (this.Entities.Contains(metadata.LogicalName) || eligibleQueryEntities.Contains(metadata.LogicalName))
                    {
                        Service.Delete(processingStep.LogicalName, processingStep.Id);
                    }
                }
            }

            Entity pluginType = this.RetrievePluginType();
            //Create steps for object type codes that do not have one already.
            foreach (string entityName in this.Entities)
            {
                foreach(string stepName in eligibleSteps)
                {
                    if(!queriedEntities.Contains(new Tuple<string, string>(entityName, stepName)))
                    {
                        EntityCollection filters = this.RetrieveSdkMessageFilters(stepName);
                        Entity message = this.RetrieveMessage(stepName);
                        EntityMetadata entityMetadata = this.MetaDataService.RetrieveMetadata(entityName);

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
