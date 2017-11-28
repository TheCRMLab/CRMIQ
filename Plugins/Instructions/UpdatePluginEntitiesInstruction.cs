using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    [InstructionName("UpdatePluginEntities")]
    public class UpdatePluginEntitiesInstruction : Instruction
    {
        public UpdatePluginEntitiesInstruction()
        {
            this.Entities = new List<string>();
        }

        [DataMember(EmitDefaultValue = true)]
        public bool AllEntities { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public List<string> Entities { get; set; }

        public static List<string> IneligibleEntities { get { return new List<string>() { "systemuser", "businessunit", "role", "savedquery", "fieldsecurityprofile", "queueitem", "userquery", "plugintype", "plugintypestatistic", "pluginassembly", "sdkmessage", "sdkmessagefilter", "sdkmessageprocessingstep", "sdkmessageprocessingstepimage", "sdkmessageprocessingstepsecureconfig", "serviceendpoint", "asyncoperation", "workflow", "tracelog", "routingrule", "routingruleitem", "report", "transactioncurrency", "mailmergetemplate", "importjob", "emailserverprofile", "mailbox", "sla", "slaitem", "slakpiinstance", "syncerror", "externalpartyitem" }; } }

        public override string Execute()
        {
            EntityCollection collection = RetrieveSdkMessageProcessingSteps();

            List<string> queriedEntities = new List<string>();
            if (this.Entities == null)
            {
                this.Entities = new List<string>();
            }

            this.Entities.RemoveAll(otc => IneligibleEntities.Contains(otc));

            foreach (Entity processingStep in collection.Entities)
            {
                if (!processingStep.Attributes.ContainsKey("a1.primaryobjecttypecode"))
                {
                    // The AllEntity one
                    if (((Microsoft.Xrm.Sdk.OptionSetValue)(processingStep.Attributes["statuscode"])).Value == 1 && !this.AllEntities)
                    {
                        // Deactivate it
                        SetStateRequest setState = new SetStateRequest();
                        setState.EntityMoniker = new EntityReference();
                        setState.EntityMoniker.Id = processingStep.Id;
                        setState.EntityMoniker.LogicalName = processingStep.LogicalName;
                        setState.State = new OptionSetValue(1);
                        setState.Status = new OptionSetValue(2);
                        this.Service.Execute(setState);
                    }
                    else if (((Microsoft.Xrm.Sdk.OptionSetValue)(processingStep.Attributes["statuscode"])).Value == 2 && this.AllEntities)
                    {
                        // Activate it
                        SetStateRequest setState = new SetStateRequest();
                        setState.EntityMoniker = new EntityReference();
                        setState.EntityMoniker.Id = processingStep.Id;
                        setState.EntityMoniker.LogicalName = processingStep.LogicalName;
                        setState.State = new OptionSetValue(0);
                        setState.Status = new OptionSetValue(1);
                        this.Service.Execute(setState);
                    }
                }
                else
                {
                    string entity = (processingStep.Attributes["a1.primaryobjecttypecode"] as AliasedValue).Value.ToString();
                    EntityMetadata metadata = this.MetaDataService.RetrieveMetadata(entity);

                    if (metadata != null)
                    {
                        queriedEntities.Add(metadata.LogicalName);
                    }

                    if (AllEntities || !this.Entities.Contains(metadata.LogicalName))
                    {
                        Service.Delete(processingStep.LogicalName, processingStep.Id);
                    }
                }
            }
            if (!this.AllEntities && this.Entities.Any(newOtc => !queriedEntities.Contains(newOtc)))
            {
                Entity message = this.RetrieveMessage();
                Entity pluginType = this.RetrievePluginType();
                EntityCollection filters = this.RetrieveSdkMessageFilters();
                
                foreach (string entityName in this.Entities.Where(newOtc => !queriedEntities.Contains(newOtc)))
                {
                    EntityMetadata entityMetadata = this.MetaDataService.RetrieveMetadata(entityName);
                    if (entityMetadata != null)
                    {
                        Entity step = new Entity("sdkmessageprocessingstep");
                        step.Attributes["eventhandler"] = new EntityReference(pluginType.LogicalName, pluginType.Id);
                        step.Attributes["name"] = String.Format("Cobalt.Components.CrmIQ.Plugin.PluginAdapter: RetrieveMultiple of {0}", entityMetadata.LogicalName);
                        step.Attributes["description"] = String.Format("Cobalt.Components.CrmIQ.Plugin.PluginAdapter: RetrieveMultiple of {0}", entityMetadata.LogicalName);
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

            return (new RetrieveExistingPluginsInstruction() { Service = this.Service }).Execute();
        }

        protected virtual Entity RetrieveMessage()
        {
            return this.ExecuteFetchScalar(
                @"<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"">
                  <entity name=""sdkmessage"">
                    <attribute name=""name"" />
                    <filter type=""and"">
                      <condition attribute=""name"" operator=""eq"" value=""RetrieveMultiple"" />
                    </filter>
                  </entity>
                </fetch>");
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

        protected EntityCollection RetrieveSdkMessageFilters()
        {
            return this.ExecuteFetchQuery(
            @"<fetch version=""1.0"" output-format=""xml-platform"" mapping=""logical"" distinct=""false"">
              <entity name=""sdkmessagefilter"">
                <attribute name=""sdkmessageid"" />
                <attribute name=""primaryobjecttypecode"" />
                <attribute name=""secondaryobjecttypecode"" />
                <attribute name=""availability"" />
                <attribute name=""iscustomprocessingstepallowed"" />
                <link-entity name=""sdkmessage"" from=""sdkmessageid"" to=""sdkmessageid"" alias=""ab"">
                  <filter type=""and"">
                    <condition attribute=""name"" operator=""eq"" value=""RetrieveMultiple"" />
                  </filter>
                </link-entity>
              </entity>
            </fetch>");
        }
    }

}
