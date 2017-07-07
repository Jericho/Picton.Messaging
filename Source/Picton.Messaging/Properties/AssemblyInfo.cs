using System.Reflection;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("Picton.Messaging")]
[assembly: AssemblyDescription("High performance async message pump for Azure")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("Picton.Messaging")]
[assembly: AssemblyCopyright("Copyright © Jeremie Desautels")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Make it easy to distinguish Debug and Release builds;
#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

// Setting ComVisible to false makes the types in this assembly not visible
// to COM components.  If you need to access a type in this assembly from
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("994163f1-9aa3-4fdf-88ff-9c373237bd9d")]
