using System;
using System.Runtime.Serialization;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    public class ConvertFetchXmlResponse
    {
        public ConvertFetchXmlResponse()
        {
        }

        [DataMember(EmitDefaultValue = false)]
        public string FetchXmlResponse { get; set; }
    }
}

