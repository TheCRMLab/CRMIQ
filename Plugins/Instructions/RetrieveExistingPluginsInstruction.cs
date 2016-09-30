using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

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
