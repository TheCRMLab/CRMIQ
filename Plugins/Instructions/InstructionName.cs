namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class InstructionName : System.Attribute
    {
        public InstructionName(string instruction)
        {
            this.Instruction = instruction;
        }
        public string Instruction { get; private set; }
    }
}
