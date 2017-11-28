using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    [InstructionName("RetrieveExistingPersistIqQueryPlugins")]
    public class RetrieveExistingPersistIqQueryPluginsInstruction : Instruction
    {
        private static readonly List<string> eligibleQueryEntities = new List<string> { "savedquery", "userquery" };

        public RetrieveExistingPersistIqQueryPluginsInstruction()
        {
        }

        public override string Execute()
        {
            EntityCollection collection = RetrieveSdkMessageProcessingStepsForPersistingIq();

            RetrieveExistingPersistIqQueryPluginsResponse response = new RetrieveExistingPersistIqQueryPluginsResponse();
            foreach (Entity processingStep in collection.Entities)
            {
                string entity = (processingStep.Attributes["a1.primaryobjecttypecode"] as AliasedValue).Value.ToString();
                if (!response.Entities.Contains(entity) && eligibleQueryEntities.Contains(entity))
                {
                    response.Entities.Add(entity);
                }
            }

            MemoryStream stream = new MemoryStream();
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(RetrieveExistingPersistIqQueryPluginsResponse));
            serializer.WriteObject(stream, response);
            stream.Position = 0;
            StreamReader streamReader = new StreamReader(stream);
            return streamReader.ReadToEnd();
        }
    }
}
