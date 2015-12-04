using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    public class RetrieveExistingPluginsResponse
    {
        public RetrieveExistingPluginsResponse()
        {
            this.ObjectTypeCodes = new List<int>();
        }

        [DataMember(EmitDefaultValue = true)]
        public bool AllEntities { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public List<int> ObjectTypeCodes { get; set; }
    }
}