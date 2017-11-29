using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.Generic;
using System.Linq;

namespace Cobalt.Components.CrmIQ.Plugin
{
    public class MetadataService
    {
        private SortedDictionary<string, EntityMetadata> crmMetadata;
        private IOrganizationService service;
        private bool allMetadataLoaded;
        public MetadataService(IOrganizationService service)
        {
            this.crmMetadata = new SortedDictionary<string, EntityMetadata>();
            this.service = service;
            this.allMetadataLoaded = false;
        }


        public EntityMetadata RetrieveMetadata(string entityName)
        {
            if (this.service != null && !string.IsNullOrEmpty(entityName))
            {
                if (!this.crmMetadata.ContainsKey(entityName))
                {
                    RetrieveEntityRequest request = new RetrieveEntityRequest() { LogicalName = entityName };
                    request.EntityFilters = Microsoft.Xrm.Sdk.Metadata.EntityFilters.Entity;
                    RetrieveEntityResponse response = (RetrieveEntityResponse)service.Execute(request);

                    if (response != null)
                    {
                        this.crmMetadata.Add(response.EntityMetadata.LogicalName, response.EntityMetadata);
                    }
                }

                if (this.crmMetadata.ContainsKey(entityName))
                {
                    return this.crmMetadata[entityName];
                }
            }

            return null;
        }

        public bool IsIntersect(string entityName)
        {
            EntityMetadata m = this.RetrieveMetadata(entityName);
            return m != null && m.IsIntersect != null && m.IsIntersect.HasValue && m.IsIntersect.Value;
        }
    }
}
