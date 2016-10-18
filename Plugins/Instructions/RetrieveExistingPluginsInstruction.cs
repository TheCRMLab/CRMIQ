using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    [InstructionName("RetrieveExistingPlugins")]
    public class RetrieveExistingPluginsInstruction : Instruction
    {
        public RetrieveExistingPluginsInstruction()
        {
        }

        public override string Execute()
        {
            EntityCollection collection = RetrieveSdkMessageProcessingSteps();

            RetrieveExistingPluginsResponse response = new RetrieveExistingPluginsResponse();
            response.AllEntities = false;
            foreach (Entity processingStep in collection.Entities)
            {
                if (!processingStep.Attributes.ContainsKey("a1.primaryobjecttypecode"))
                {
                    response.AllEntities = ((Microsoft.Xrm.Sdk.OptionSetValue)(processingStep.Attributes["statuscode"])).Value == 1;
                }
                else
                {
                    string entity = (processingStep.Attributes["a1.primaryobjecttypecode"] as AliasedValue).Value.ToString();
                    if (this.MetaDataService != null)
                    {
                        EntityMetadata metadata = this.MetaDataService.RetrieveMetadata(entity);
                        if (metadata != null && metadata.ObjectTypeCode != null && metadata.ObjectTypeCode.HasValue)
                        {
                            response.ObjectTypeCodes.Add(metadata.ObjectTypeCode.Value);
                        }
                    }
                }
            }

            MemoryStream stream = new MemoryStream();
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(RetrieveExistingPluginsResponse));
            serializer.WriteObject(stream, response);
            stream.Position = 0;
            StreamReader streamReader = new StreamReader(stream);
            string returnValue = streamReader.ReadToEnd();
            
            return returnValue;
        }
    }
}
