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
            this.ObjectTypeCodes = new List<int>();
        }

        [DataMember(EmitDefaultValue = true)]
        public bool AllEntities { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public List<int> ObjectTypeCodes { get; set; }

        protected List<int> IneligibleEntities { get { return new List<int>() { 8, 10, 1036, 1039, 1200, 2029, 4230, 4602, 4603, 4605, 4606, 4607, 4608, 4615, 4616, 4618, 4700, 4703, 8050, 8181, 8199, 9100, 9105, 9106, 9107, 9605, 9606, 9750, 9751, 9752, 9869, 9987 }; } }

        public override string Execute()
        {
            EntityCollection collection = RetrieveSdkMessageProcessingSteps();

            List<int> queriedEntities = new List<int>();
            if (this.ObjectTypeCodes == null)
            {
                this.ObjectTypeCodes = new List<int>();
            }

            this.ObjectTypeCodes.RemoveAll(otc => this.IneligibleEntities.Contains(otc));

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

                    if (metadata != null && metadata.ObjectTypeCode != null && metadata.ObjectTypeCode.HasValue)
                    {
                        queriedEntities.Add(metadata.ObjectTypeCode.Value);
                    }

                    if (AllEntities || !this.ObjectTypeCodes.Contains(metadata.ObjectTypeCode.Value))
                    {
                        Service.Delete(processingStep.LogicalName, processingStep.Id);
                    }
                }
            }
            if (!this.AllEntities && this.ObjectTypeCodes.Any(newOtc => !queriedEntities.Contains(newOtc)))
            {
                Entity message = this.RetrieveMessage();
                Entity pluginType = this.RetrievePluginType();
                EntityCollection filters = this.RetrieveSdkMessageFilters();
                
                foreach (int objectTypeCode in this.ObjectTypeCodes.Where(newOtc => !queriedEntities.Contains(newOtc)))
                {
                    EntityMetadata entityMetadata = this.MetaDataService.RetrieveMetadataByObjectTypeCode(objectTypeCode);
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
