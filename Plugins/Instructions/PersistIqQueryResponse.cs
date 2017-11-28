using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    public class PersistIqQueryResponse
    {
        public PersistIqQueryResponse()
        {
            this.Entities = new List<string>();
        }

        [DataMember(EmitDefaultValue = false)]
        public List<string> Entities { get; set; }
    }
}
