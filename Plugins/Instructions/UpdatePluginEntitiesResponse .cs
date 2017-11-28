using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    public class UpdatePluginEntitiesResponse  
    {
        public UpdatePluginEntitiesResponse  ()
        {
            this.Entities = new List<string>();
        }

        [DataMember(EmitDefaultValue = true)]
        public bool AllEntities { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public List<string> Entities { get; set; }
    }
}