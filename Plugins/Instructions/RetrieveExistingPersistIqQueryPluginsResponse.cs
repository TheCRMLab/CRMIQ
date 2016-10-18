﻿using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    public class RetrieveExistingPersistIqQueryPluginsResponse
    {
        public RetrieveExistingPersistIqQueryPluginsResponse()
        {
            this.ObjectTypeCodes = new List<int>();
        }

        [DataMember(EmitDefaultValue = false)]
        public List<int> ObjectTypeCodes { get; set; }
    }
}